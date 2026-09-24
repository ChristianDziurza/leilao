use leilao::proto::leilao_server::{Leilao, LeilaoServer};
use leilao::proto::{
    AcompanharRequest, AtualizacaoLeilao, HistoricoRequest, HistoricoResponse, Lance, LanceRequest,
    LanceResponse,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

const DURACAO_LEILAO_SEGUNDOS: u64 = 60;

struct EstadoLeilao {
    item: String,
    maior_lance: f64,
    maior_lance_de: String,
    encerrado: bool,
    historico: Vec<Lance>,
}

struct GerenciadorLeilao {
    leiloes: HashMap<String, EstadoLeilao>,
    tx_eventos: HashMap<String, broadcast::Sender<AtualizacaoLeilao>>,
}

struct LeilaoService {
    estado: Arc<Mutex<GerenciadorLeilao>>,
}

#[tonic::async_trait]
impl Leilao for LeilaoService {
    async fn dar_lance(
        &self,
        request: Request<LanceRequest>,
    ) -> Result<Response<LanceResponse>, Status> {
        let req = request.into_inner();
        let mut gerenciador = self.estado.lock().await;

        let estado = match gerenciador.leiloes.get_mut(&req.id_leilao) {
            Some(l) => l,
            None => {
                return Ok(Response::new(LanceResponse {
                    aceito: false,
                    mensagem: "Leilao nao encontrado.".to_string(),
                    maior_lance_atual: 0.0,
                }))
            }
        };

        if estado.encerrado {
            return Ok(Response::new(LanceResponse {
                aceito: false,
                mensagem: "Leilao ja encerrado.".to_string(),
                maior_lance_atual: estado.maior_lance,
            }));
        }

        if req.valor <= estado.maior_lance {
            return Ok(Response::new(LanceResponse {
                aceito: false,
                mensagem: format!("Lance invalido. Deve ser maior que {:.2}", estado.maior_lance),
                maior_lance_atual: estado.maior_lance,
            }));
        }

        estado.maior_lance = req.valor;
        estado.maior_lance_de = req.participante.clone();

        let novo_lance = Lance {
            participante: req.participante.clone(),
            valor: req.valor,
        };
        estado.historico.push(novo_lance);

        println!(
            "Lance de R$ {:.2} recebido de {} no leilao {}",
            req.valor, req.participante, req.id_leilao
        );

        if let Some(tx) = gerenciador.tx_eventos.get(&req.id_leilao) {
            let _ = tx.send(AtualizacaoLeilao {
                participante: req.participante.clone(),
                valor: req.valor,
                evento: "novo_lance".to_string(),
            });
        }

        Ok(Response::new(LanceResponse {
            aceito: true,
            mensagem: "Lance efetuado.".to_string(),
            maior_lance_atual: req.valor,
        }))
    }

    type AcompanharLeilaoStream =
        Pin<Box<dyn Stream<Item = Result<AtualizacaoLeilao, Status>> + Send + 'static>>;

    async fn acompanhar_leilao(
        &self,
        request: Request<AcompanharRequest>,
    ) -> Result<Response<Self::AcompanharLeilaoStream>, Status> {
        let req = request.into_inner();
        let gerenciador = self.estado.lock().await;

        let tx = gerenciador
            .tx_eventos
            .get(&req.id_leilao)
            .ok_or_else(|| Status::not_found("Leilao nao encontrado"))?;

        println!("Cliente {} acompanhando leilao {}", req.participante, req.id_leilao);

        let rx = tx.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
            .filter_map(|item| item.ok())
            .map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn obter_historico(
        &self,
        request: Request<HistoricoRequest>,
    ) -> Result<Response<HistoricoResponse>, Status> {
        let req = request.into_inner();
        let gerenciador = self.estado.lock().await;

        let estado = gerenciador
            .leiloes
            .get(&req.id_leilao)
            .ok_or_else(|| Status::not_found("Leilao nao encontrado"))?;

        Ok(Response::new(HistoricoResponse {
            lances: estado.historico.clone(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;

    let mut leiloes = HashMap::new();
    let mut tx_eventos = HashMap::new();

    let ids = vec!["leilao-1", "leilao-2"];
    let itens = vec!["Quadro Raro", "Relogio Antigo"];

    for (id, item) in ids.into_iter().zip(itens.into_iter()) {
        let (tx, _rx) = broadcast::channel(32);
        leiloes.insert(
            id.to_string(),
            EstadoLeilao {
                item: item.to_string(),
                maior_lance: 0.0,
                maior_lance_de: "ninguem".to_string(),
                encerrado: false,
                historico: Vec::new(),
            },
        );
        tx_eventos.insert(id.to_string(), tx);
    }

    let gerenciador = Arc::new(Mutex::new(GerenciadorLeilao {
        leiloes,
        tx_eventos,
    }));

    for id in vec!["leilao-1", "leilao-2"] {
        let gerenciador_clone = gerenciador.clone();
        let id_leilao = id.to_string();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(DURACAO_LEILAO_SEGUNDOS)).await;
            let mut g = gerenciador_clone.lock().await;

            let dados_encerramento = if let Some(estado) = g.leiloes.get_mut(&id_leilao) {
                estado.encerrado = true;
                println!(
                    "Leilao {} encerrado. Vencedor: {} com R$ {:.2}",
                    id_leilao, estado.maior_lance_de, estado.maior_lance
                );
                Some((estado.maior_lance_de.clone(), estado.maior_lance))
            } else {
                None
            };

            if let Some((vencedor, valor)) = dados_encerramento {
                if let Some(tx) = g.tx_eventos.get(&id_leilao) {
                    let _ = tx.send(AtualizacaoLeilao {
                        participante: vencedor,
                        valor,
                        evento: "leilao_encerrado".to_string(),
                    });
                }
            }
        });
    }

    println!("Servidor rodando em {}", addr);

    let servico = LeilaoService {
        estado: gerenciador,
    };

    Server::builder()
        .add_service(LeilaoServer::new(servico))
        .serve(addr)
        .await?;

    Ok(())
}