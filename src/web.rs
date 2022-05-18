//! web server for exastash

use anyhow::{anyhow, ensure, Error, Result};
use axum::{
    routing::{get, post},
    extract::Path,
    http::StatusCode,
    response::{Response, IntoResponse},
    Json, Router,
};
use axum_macros::debug_handler;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use crate::util;

mod errors {

}

/// Start a web server with fofs serving capabilities
pub async fn run(port: u16) -> Result<()> {
    // build our application with a route
    let app = Router::new()
        .route("/", get(root))
        .route("/fofs/:pile_id/:cell_id/:file_id", get(fofs_get));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn root() -> String {
    format!("es web on {}", util::get_hostname())
}

struct AppError(anyhow::Error);

impl From<Error> for AppError {
    fn from(inner: Error) -> Self {
        AppError(inner)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

type AppResult = std::result::Result<Response, AppError>;

#[debug_handler]
async fn fofs_get(
    Path(pile_id): Path<i64>,
    Path(cell_id): Path<i64>,
    Path(file_id): Path<i64>,
) -> AppResult {
    try {
        ensure!(pile_id >= 1, "pile_id was {}", pile_id);
        Ok((StatusCode::OK, "OK").into_response())
    }?
}
