//! web server for exastash

use std::net::SocketAddr;
use tokio_util::io::ReaderStream;
use axum::{
    body::StreamBody,
    routing::{get, post},
    extract::Path,
    http::{StatusCode, Uri},
    response::{Response, IntoResponse},
    handler::Handler,
    Router, Extension,
};
use tower::ServiceBuilder;
use tracing::info;
use std::{
    collections::HashMap,
    sync::Arc,
};
use futures::lock::Mutex;
use axum_macros::debug_handler;
use crate::util;
use crate::db;

/// Start a web server with fofs serving capabilities
pub async fn run(port: u16) -> Result<(), hyper::Error> {
    // build our application with a route
    let app = Router::new()
        .route("/", get(root))
        .route("/fofs/:pile_id/:cell_id/:file_id", get(fofs_get))
        // Don't let axum serve with trailing slash. Thanks axum.
        // https://github.com/tokio-rs/axum/pull/410/files
        .route("/fofs/:pile_id/:cell_id/:file_id/", get(not_found))
        .fallback(fallback.into_service())
        .layer(
            ServiceBuilder::new()
                .layer(Extension(SharedState::default()))
                .into_inner(),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
}

async fn root() -> String {
    format!("es web on {}", util::get_hostname())
}

/// Errors used by our web server
#[derive(thiserror::Error, Debug)]
#[allow(variant_size_differences)]
pub enum Error {
    /// Access forbidden
    #[error("access forbidden")]
    Forbidden,

    /// Access forbidden
    #[error("route not found")]
    NoSuchRoute,

    /// Bad request
    #[error("bad request")]
    BadRequest,

    /// File was not found
    #[error("file not found")]
    FileNotFound,

    /// Pile was not found
    #[error("pile not found")]
    PileNotFound,

    /// Pile was not found
    #[error("pile was found, but it's not on this machine")]
    PileNotOnThisMachine,

    /// A problem with the database
    #[error("an error occurred with the database")]
    Sqlx(#[from] sqlx::Error),

    /// Some problem doing IO
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Some other error created by anyhow
    #[error("an internal server error occurred")]
    Anyhow(#[from] anyhow::Error),
}

impl Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NoSuchRoute => StatusCode::NOT_FOUND,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::FileNotFound => StatusCode::NOT_FOUND,
            Self::PileNotFound => StatusCode::NOT_FOUND,
            Self::PileNotOnThisMachine => StatusCode::NOT_FOUND,
            Self::Io(e) if e.kind() == std::io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
            Self::Sqlx(_) | Self::Anyhow(_) | Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        // Log some errors
        match self {
            Self::Sqlx(ref e) => {
                log::error!("SQLx error: {:?}", e);
            }
            Self::Anyhow(ref e) => {
                log::error!("Generic error: {:?}", e);
            }
            _ => (),
        }
        (self.status_code(), self.to_string()).into_response()
    }
}

async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

async fn fallback(_: Uri) -> impl IntoResponse {
    Error::NoSuchRoute
}

type FofsPilePaths = HashMap<i32, String>;

#[derive(Default)]
struct State {
    fofs_pile_paths: FofsPilePaths,
}

type SharedState = Arc<Mutex<State>>;

async fn get_fofs_pile_path(pile_id: i32) -> Result<String, Error> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;
    let mut piles = db::storage::fofs::Pile::find_by_ids(&mut transaction, &[pile_id]).await?;
    let pile = match piles.pop() {
        Some(pile) => pile,
        None => return Err(Error::PileNotFound),
    };
    if pile.hostname != util::get_hostname() {
        return Err(Error::PileNotOnThisMachine)
    }
    Ok(pile.path)
}

/// Note that we sort of trust the client here and allow them to
/// fetch any {cell_id}/{file_id} file a local pile might have,
/// even if it isn't in the database for some reason.
#[debug_handler]
async fn fofs_get(
    // TODO: don't allow leading 0's on the path parameters
    Path((pile_id, cell_id, file_id)): Path<(i32, i32, i64)>,
    Extension(state): Extension<SharedState>,
) -> Result<Response, Error> {
    if pile_id < 1 || cell_id < 1 || file_id < 1 {
        return Err(Error::BadRequest);
    }

    let cached_pile_path = {
        let mut lock = state.lock().await;
        let fofs_pile_paths = &mut lock.fofs_pile_paths;
        fofs_pile_paths.get(&pile_id).cloned()
    };
    let pile_path: String = match cached_pile_path {
        Some(path) => path,
        None => {
            info!(pile_id, "looking up pile path");
            let path = get_fofs_pile_path(pile_id).await?;
            let mut lock = state.lock().await;
            let fofs_pile_paths = &mut lock.fofs_pile_paths;
            fofs_pile_paths.insert(pile_id, path.clone());
            path
        }
    };

    let fname = format!("{}/{}/{}/{}", pile_path, pile_id, cell_id, file_id);
    let fofs_file_size = tokio::fs::metadata(&fname).await?.len();
    let file = tokio::fs::File::open(fname).await?;
    let stream = ReaderStream::new(file);
    let body = axum::body::boxed(StreamBody::new(stream));
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", fofs_file_size)
        .body(body)
        .unwrap();
    Ok(response)
}
