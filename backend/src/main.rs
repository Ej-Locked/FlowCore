use actix::prelude::*;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws::{self, Message, ProtocolError, WebsocketContext};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::time::Duration;
use uuid::Uuid;
use std::fs;
use log::{info, warn};
use rand::Rng;
use chrono::Utc;
use rand::SeedableRng;
use rand::rngs::StdRng;


#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    id: String,
    ts: i64, // epoch millis
    value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowResult {
    window_start: i64,
    window_end: i64,
    count: usize,
    sum: f64,
}

#[derive(Clone)]
struct AppState {
    // window_start -> events
    windows: Arc<Mutex<HashMap<i64, Vec<Event>>>>,
    // current watermark (epoch millis)
    watermark: Arc<Mutex<i64>>,
    // allowed lateness in millis
    allowed_lateness_ms: i64,
}

impl AppState {
    fn new(allowed_lateness_ms: i64) -> Self {
        AppState {
            windows: Arc::new(Mutex::new(HashMap::new())),
            watermark: Arc::new(Mutex::new(0)),
            allowed_lateness_ms,
        }
    }
}

async fn ingest(evt: web::Json<Event>, data: web::Data<AppState>) -> impl Responder {
    let e = evt.into_inner();
    let mut windows = data.windows.lock().unwrap();
    // tumbling window size 10s
    let win_size_ms = 10_000;
    let win_start = (e.ts / win_size_ms) * win_size_ms;
    windows.entry(win_start).or_insert_with(Vec::new).push(e.clone());

    // update watermark if event timestamp greater
    {
        let mut wm = data.watermark.lock().unwrap();
        if e.ts > *wm {
            *wm = e.ts;
        }
    }

    HttpResponse::Ok().json(serde_json::json!({"status":"accepted","id":e.id}))
}

async fn health() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

// WebSocket actor
struct MyWs;
impl Actor for MyWs {
    type Context = WebsocketContext<Self>;
    fn started(&mut self, _ctx: &mut Self::Context) {
        // nothing special now, placeholder
    }
}

impl StreamHandler<Result<Message, ProtocolError>> for MyWs {
    fn handle(&mut self, msg: Result<Message, ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(Message::Ping(p)) => {
                ctx.pong(&p);
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Text(_t)) => {
                // optionally handle inbound text messages from client
            }
            Ok(Message::Binary(_b)) => {}
            Ok(Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Err(_e) => {
                ctx.stop();
            }
            _ => {}
        }
    }
}

async fn ws_index(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, actix_web::Error> {
    ws::start(MyWs {}, &req, stream)
}

// Simple checkpointing: dump windows and watermark to disk periodically
fn checkpoint_state(base: &str, windows: &HashMap<i64, Vec<Event>>, watermark: i64) -> anyhow::Result<()> {
    let obj = serde_json::json!({
        "watermark": watermark,
        "windows": windows,
    });
    fs::create_dir_all(base)?;
    fs::write(format!("{}/checkpoint.json", base), serde_json::to_string_pretty(&obj)?)?;
    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let state = AppState::new(5_000); // allowed lateness 5 seconds
    let data = web::Data::new(state.clone());

    // Spawn background task to periodically advance watermark, close windows, emit results, and checkpoint.
    {
        let state_bg = state.clone();
        tokio::spawn(async move {
            let win_size_ms = 10_000;
            loop {
                // compute watermark as max_event_ts - allowed_lateness
                let max_ts = { *state_bg.watermark.lock().unwrap() };
                let watermark = if max_ts == 0 { 0 } else { max_ts - state_bg.allowed_lateness_ms };
                {
                    let mut wm_lock = state_bg.watermark.lock().unwrap();
                    *wm_lock = watermark;
                }

                // collect windows that have window_end <= watermark
                let mut to_emit: Vec<WindowResult> = Vec::new();
                {
                    let mut windows = state_bg.windows.lock().unwrap();
                    let keys: Vec<i64> = windows.keys().cloned().collect();
                    for &start in &keys {
                        let end = start + win_size_ms;
                        if end <= watermark && watermark > 0 {
                            if let Some(evts) = windows.remove(&start) {
                                let count = evts.len();
                                let sum: f64 = evts.iter().map(|e| e.value).sum();
                                to_emit.push(WindowResult { window_start: start, window_end: end, count, sum });
                            }
                        }
                    }
                }

                // emit results: write to disk as a simple "output log" that frontend can poll/WS read
                if !to_emit.is_empty() {
                    let out_dir = "/tmp/flowCore_out";
                    fs::create_dir_all(out_dir).ok();
                    for r in to_emit {
                        let line = serde_json::to_string(&r).unwrap();
                        // append to file
                        let log_path = format!("{}/results.log", out_dir);
                        use std::fs::OpenOptions;
                        let mut f = OpenOptions::new().create(true).append(true).open(&log_path).unwrap();
                        use std::io::Write;
                        writeln!(f, "{}", line).ok();
                        info!("Emitted window result: {} - {} count={} sum={}", r.window_start, r.window_end, r.count, r.sum);
                    }
                }

                // checkpoint
                {
                    let windows = state_bg.windows.lock().unwrap();
                    let watermark_now = *state_bg.watermark.lock().unwrap();
                    if let Err(e) = checkpoint_state("/tmp/flowCore_ckpt", &*windows, watermark_now) {
                        warn!("Checkpoint failed: {}", e);
                    } else {
                        info!("Checkpoint saved. watermark={}", watermark_now);
                    }
                }

                tokio::time::sleep(Duration::from_millis(2000)).await;
            }
        });
    }

    // spawn generator to create example events (for demo)
    {
        let data_gen = data.clone();
        tokio::spawn(async move {
            // StdRng is Send — safe to keep across awaits
            let mut rng = StdRng::from_entropy();
            loop {
                let now = Utc::now().timestamp_millis();
                let evt = Event {
                    id: Uuid::new_v4().to_string(),
                    ts: now,
                    value: (rng.gen_range(0.0_f64..100.0_f64) * 100.0_f64).round() / 100.0_f64,
                };

                // add into state
                {
                    let mut windows = data_gen.windows.lock().unwrap();
                    let win_size_ms = 10_000;
                    let win_start = (evt.ts / win_size_ms) * win_size_ms;
                    windows.entry(win_start).or_insert_with(Vec::new).push(evt.clone());
                    let mut wm = data_gen.watermark.lock().unwrap();
                    if evt.ts > *wm { *wm = evt.ts; }
                }

                tokio::time::sleep(Duration::from_millis(1300)).await;
            }
        });
    }

    // create a tiny file poller endpoint for frontend to fetch recent results and late events
    async fn recent() -> impl Responder {
        let out_dir = "/tmp/flowCore_out";
        let log_path = format!("{}/results.log", out_dir);
        if let Ok(s) = fs::read_to_string(&log_path) {
            HttpResponse::Ok().body(s)
        } else {
            HttpResponse::Ok().body("")
        }
    }

    // late events endpoint: check windows and watermark; scan for any events older than watermark
    async fn late_events(data: web::Data<AppState>) -> impl Responder {
        let wm = *data.watermark.lock().unwrap();
        let mut late: Vec<Event> = Vec::new();
        {
            let windows = data.windows.lock().unwrap();
            for (_k, vec) in windows.iter() {
                for e in vec {
                    if e.ts + data.allowed_lateness_ms < wm {
                        late.push(e.clone());
                    }
                }
            }
        }
        HttpResponse::Ok().json(late)
    }

    // server
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/ingest", web::post().to(ingest))
            .route("/health", web::get().to(health))
            .route("/ws/", web::get().to(ws_index))
            .route("/recent", web::get().to(recent))
            .route("/late", web::get().to(late_events))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
