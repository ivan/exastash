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
    Router,
};
#[allow(unused)]
use axum_macros::debug_handler;
use crate::util;

/// Start a web server with fofs serving capabilities
pub async fn run(port: u16) -> Result<(), hyper::Error> {
    // build our application with a route
    let app = Router::new()
        .route("/", get(root))
        .route("/fofs/:pile_id/:cell_id/:file_id", get(fofs_get))
        // Don't let axum serve with trailing slash. Thanks axum.
        // https://github.com/tokio-rs/axum/pull/410/files
        .route("/fofs/:pile_id/:cell_id/:file_id/", get(not_found))
        .fallback(fallback.into_service());

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

/// Note that we sort of trust the client here and allow them to
/// fetch any /fofs/{pile_id}/{cell_id}/{file_id} file we have,
/// even if it isn't in the database.
async fn fofs_get(
    // TODO: don't allow leading 0's on the path parameters
    Path((pile_id, cell_id, file_id)): Path<(i64, i64, i64)>,
) -> Result<Response, Error> {
    if pile_id < 1 || cell_id < 1 || file_id < 1 {
        return Err(Error::BadRequest);
    }

    // TODO: maintain in-memory cache of fofs pile paths instead of assuming /fofs
    let pile_path = "/fofs";
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
