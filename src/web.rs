//! web server for exastash

use std::{net::SocketAddr, str::FromStr};
use hyper::Request;
use tokio_util::io::ReaderStream;
use axum::{
    middleware,
    body::StreamBody,
    routing::{get, post},
    extract::Path,
    http::{StatusCode, Uri, HeaderValue},
    response::{Response, IntoResponse},
    handler::Handler,
    Router, Extension, middleware::Next,
};
use tower::ServiceBuilder;
use tracing::info;
use std::{
    collections::HashMap,
    sync::Arc,
};
use std::fmt;
use once_cell::sync::Lazy;
use futures::lock::Mutex;
use axum_macros::debug_handler;
use serde::{de, Deserialize, Deserializer};
use smol_str::SmolStr;
use crate::util;
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
    ParseNaturalNumber,
}

impl Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NoSuchRoute => StatusCode::NOT_FOUND,
            Self::ParseNaturalNumber => StatusCode::BAD_REQUEST,
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

type FofsPilePaths = HashMap<i32, SmolStr>;

#[derive(Default)]
struct State {
    fofs_pile_paths: FofsPilePaths,
}

type SharedState = Arc<Mutex<State>>;

async fn get_fofs_pile_path(pile_id: i32) -> Result<SmolStr, Error> {
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
    Ok(pile.path.into())
}

/// Parse strictly, forbidding leading '0' or '+'
#[inline]
fn parse_natural_number<T: FromStr>(s: &str) -> Result<T, Error> {
    if s.starts_with('0') || s.starts_with('+') {
        return Err(Error::ParseNaturalNumber)
    }
    s.parse::<T>().map_err(|_| Error::ParseNaturalNumber)
}

fn serde_parse_natural_number<'de, D, T: FromStr>(de: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    let s = SmolStr::deserialize(de)?;
    parse_natural_number(&s).map_err(de::Error::custom)
}

/// Strictly-parsed natural number, forbidding leading '0' or '+'
#[derive(Debug, Deserialize)]
struct NatNum<T: FromStr> (
    #[serde(default, deserialize_with = "serde_parse_natural_number")]
    T
) where T::Err: fmt::Display;

/// Note that we sort of trust the client here and allow them to
/// fetch any {cell_id}/{file_id} file a local pile might have,
/// even if it isn't in the database for some reason.
#[debug_handler]
async fn fofs_get(
    Path((NatNum(pile_id), NatNum(cell_id), NatNum(file_id))): Path<(NatNum<i32>, NatNum<i32>, NatNum<i64>)>,
    Extension(state): Extension<SharedState>,
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

    let fname = format!("{}/{}/{}/{}", pile_path, pile_id, cell_id, file_id);
    let fofs_file_size = tokio::fs::metadata(&fname).await?.len();
    let file = tokio::fs::File::open(fname).await?;
    let stream = ReaderStream::new(file);
    let body = axum::body::boxed(StreamBody::new(stream));
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-length", fofs_file_size)
        .header("content-type", "application/octet-stream")
        .body(body)
        .unwrap();
    Ok(response)
}

async fn root() -> String {
    format!("es web on {}", util::get_hostname())
}

static SERVER: Lazy<HeaderValue> = Lazy::new(|| {
    let version = env!("CARGO_PKG_VERSION");
    let s = format!("es web/{version}");
    s.try_into().unwrap()
});

async fn add_common_headers<B>(req: Request<B>, next: Next<B>) -> Response {
    let mut response = next.run(req).await;
    response.headers_mut().insert("server", SERVER.clone());
    response
}

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
                .layer(middleware::from_fn(add_common_headers))
                .into_inner(),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
}
