//! web server for exastash

use axum::{
    routing::{get, post},
    extract::Path,
    http::StatusCode,
    response::{Response, IntoResponse},
    Router,
};
use axum_macros::debug_handler;
use std::net::SocketAddr;
use crate::util;

/// Start a web server with fofs serving capabilities
pub async fn run(port: u16) -> Result<(), hyper::Error> {
    // build our application with a route
    let app = Router::new()
        .route("/", get(root))
        .route("/fofs/:pile_id/:cell_id/:file_id", get(fofs_get));

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

    /// Bad request
    #[error("bad request")]
    BadRequest,

    /// A problem with the database
    #[error("an error occurred with the database")]
    Sqlx(#[from] sqlx::Error),

    /// Some other error created by anyhow
    #[error("an internal server error occurred")]
    Anyhow(#[from] anyhow::Error),
}

impl Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::Sqlx(_) | Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
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

#[debug_handler]
async fn fofs_get(
    Path((pile_id, cell_id, file_id)): Path<(i64, i64, i64)>,
) -> Result<Response, Error> {
    if pile_id < 1 || cell_id < 1 || file_id < 1 {
        return Err(Error::BadRequest);
    }
    Ok((StatusCode::OK, "OK").into_response())
}
