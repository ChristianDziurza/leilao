use leilao::proto::leilao_client::LeilaoClient;
use leilao::proto::{AcompanharRequest, HistoricoRequest, LanceRequest};
use std::env;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let nome = args.get(1).cloned().unwrap_or_else(|| "Anonimo".to_string());
    let id_leilao = args.get(2).cloned().unwrap_or_else(|| "leilao-1".to_string());
    let endereco = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:50051".to_string());

    let mut client = LeilaoClient::connect(endereco.clone()).await?;
    println!("Conectado em {} | Usuario: {} | Leilao: {}", endereco, nome, id_leilao);

    let mut client_stream = client.clone();
    let nome_stream = nome.clone();
    let id_leilao_stream = id_leilao.clone();

    tokio::spawn(async move {
        let resposta = client_stream
            .acompanhar_leilao(AcompanharRequest {
                id_leilao: id_leilao_stream,
                participante: nome_stream,
            })
            .await;

        match resposta {
            Ok(res) => {
                let mut stream = res.into_inner();
                while let Ok(Some(atualizacao)) = stream.message().await {
                    if atualizacao.evento == "leilao_encerrado" {
                        println!(
                            "Leilao encerrado. Vencedor: {} com R$ {:.2}",
                            atualizacao.participante, atualizacao.valor
                        );
                        break;
                    } else {
                        println!(
                            "Novo lance no leilao: {} ofereceu R$ {:.2}",
                            atualizacao.participante, atualizacao.valor
                        );
                    }
                }
            }
            Err(e) => eprintln!("Erro na stream de atualizacoes: {}", e),
        }
    });

    println!("Comandos: digite um valor para dar um lance, 'historico' para ver lances ou 'sair'");

    let stdin = tokio::io::stdin();
    let mut linhas = BufReader::new(stdin).lines();

    while let Some(linha) = linhas.next_line().await? {
        let linha = linha.trim();

        if linha.eq_ignore_ascii_case("sair") {
            break;
        }

        if linha.eq_ignore_ascii_case("historico") {
            match client
                .obter_historico(HistoricoRequest {
                    id_leilao: id_leilao.clone(),
                })
                .await
            {
                Ok(resp) => {
                    let h = resp.into_inner();
                    println!("--- Historico do leilao {} ---", id_leilao);
                    for l in h.lances {
                        println!("- {}: R$ {:.2}", l.participante, l.valor);
                    }
                }
                Err(e) => println!("Erro ao obter historico: {}", e),
            }
            continue;
        }

        match linha.parse::<f64>() {
            Ok(valor) => {
                let resposta = client
                    .dar_lance(LanceRequest {
                        id_leilao: id_leilao.clone(),
                        participante: nome.clone(),
                        valor,
                    })
                    .await?;
                let r = resposta.into_inner();
                println!("Resposta: {} (maior lance: R$ {:.2})", r.mensagem, r.maior_lance_atual);
            }
            Err(_) => println!("Entrada invalida. Digite um numero, 'historico' ou 'sair'."),
        }
    }

    Ok(())
}