//! web server for exastash

use std::net::SocketAddr;
use tokio_util::io::ReaderStream;
use axum::{
    middleware::{self, Next},
    debug_handler,
    body::Body,
    routing::get,
    extract::{Request, Path, State},
    http::{StatusCode, Uri, HeaderValue},
    response::{Response, IntoResponse},
    Router,
};
use tracing::info;
use std::{
    collections::HashMap,
    sync::Arc,
};
use once_cell::sync::Lazy;
use futures::lock::Mutex;
use smol_str::SmolStr;
use crate::util::{self, NatNum};
use crate::db;

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

    /// Pile was found, but it's not on this machine
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

    /// Some number given could not be parsed
    #[error("number could not be parsed strictly as a natural number")]
    ParseNaturalNumber(#[from] util::ParseNaturalNumberError),
}

impl Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NoSuchRoute => StatusCode::NOT_FOUND,
            Self::ParseNaturalNumber(_) => StatusCode::BAD_REQUEST,
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

async fn fallback(_: Uri) -> impl IntoResponse {
    Error::NoSuchRoute
}

type FofsPilePaths = HashMap<i32, SmolStr>;

#[derive(Default)]
struct FofsState {
    fofs_pile_paths: FofsPilePaths,
}

type SharedFofsState = Arc<Mutex<FofsState>>;

async fn get_fofs_pile_path(pile_id: i32) -> Result<SmolStr, Error> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;
    let mut piles = db::storage::fofs::Pile::find_by_ids(&mut transaction, &[pile_id]).await?;
    transaction.commit().await?; // close read-only transaction
    let pile = match piles.pop() {
        Some(pile) => pile,
        None => return Err(Error::PileNotFound),
    };
    if pile.hostname != util::get_hostname() {
        return Err(Error::PileNotOnThisMachine);
    }
    Ok(pile.path.into())
}

/// Note that we sort of trust the client here and allow them to
/// fetch any {cell_id}/{file_id} file a local pile might have,
/// even if it isn't in the database for some reason.
#[debug_handler]
async fn fofs_get(
    Path((NatNum(pile_id), NatNum(cell_id), NatNum(file_id))): Path<(NatNum<i32>, NatNum<i32>, NatNum<i64>)>,
    State(state): State<SharedFofsState>,
) -> Result<Response, Error> {
    let cached_pile_path = {
        let mut lock = state.lock().await;
        let fofs_pile_paths = &mut lock.fofs_pile_paths;
        fofs_pile_paths.get(&pile_id).cloned()
    };
    let pile_path: SmolStr = match cached_pile_path {
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

    let fname = format!("{pile_path}/{pile_id}/{cell_id}/{file_id}");
    let fofs_file_size = tokio::fs::metadata(&fname).await?.len();
    let file = tokio::fs::File::open(fname).await?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-length", fofs_file_size)
        .header("content-type", "application/octet-stream")
        .body(body)
        .unwrap();
    Ok(response)
}

static SERVER: Lazy<HeaderValue> = Lazy::new(|| {
    let version = env!("CARGO_PKG_VERSION");
    let s = format!("es web/{version}");
    s.try_into().unwrap()
});

async fn add_common_headers(req: Request, next: Next) -> Response {
    let mut response = next.run(req).await;
    response.headers_mut().insert("server", SERVER.clone());
    response
}

async fn root() -> String {
    format!("{} on {}", SERVER.to_str().unwrap(), util::get_hostname())
}

/// Start a web server with fofs serving capabilities
pub async fn run(port: u16) -> anyhow::Result<()> {
    let state = SharedFofsState::default();
    let app = Router::new()
        .route("/", get(root))
        .route("/fofs/:pile_id/:cell_id/:file_id", get(fofs_get))
        .fallback(fallback)
        .with_state(state)
        .layer(middleware::from_fn(add_common_headers));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
