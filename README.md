# Leilão distribuído em Rust + gRPC (tonic)

Aplicação simples de RPC que simula um leilão. Servidor central em Rust
com `tonic`/`prost` e clientes em Rust que dão lances e acompanham o
leilão em tempo real.

## Estrutura

```
leilao/
├── Cargo.toml
├── build.rs                # compila o .proto em código Rust na hora do build
├── proto/leilao.proto      # contrato do serviço RPC
└── src/
    ├── lib.rs               # expõe o módulo gerado pelo tonic-build
    └── bin/
        ├── servidor.rs      # servidor gRPC
        └── cliente.rs       # cliente gRPC (linha de comando)
```

## O serviço RPC (`proto/leilao.proto`)

```protobuf
service Leilao {
  rpc DarLance (LanceRequest) returns (LanceResponse);
  rpc AcompanharLeilao (AcompanharRequest) returns (stream AtualizacaoLeilao);
}
```

Dois estilos de RPC, de propósito, para dar conteúdo à apresentação:

- **`DarLance` — RPC unário.** O cliente manda um lance, o servidor responde
  na hora se foi aceito ou não. Igual uma chamada de função normal, só que
  atravessando a rede.
- **`AcompanharLeilao` — RPC server-streaming.** O cliente faz UMA chamada,
  mas o servidor pode responder com VÁRIAS mensagens ao longo do tempo
  (a cada novo lance de qualquer participante, e quando o leilão acaba).
  Por baixo dos panos isso usa uma única conexão HTTP/2 aberta, por onde o
  servidor vai empurrando eventos.

## Como o RPC funciona aqui (para a apresentação)

1. **`.proto` → código Rust:** o `build.rs` roda o `tonic-build` durante a
   compilação e gera automaticamente, a partir de `leilao.proto`, as
   structs (`LanceRequest`, `AtualizacaoLeilao`, ...) e os traits
   `Leilao` (servidor) / `LeilaoClient` (cliente).
2. **Servidor:** implementa o trait `Leilao` gerado, com o estado real
   do leilão (`maior_lance`, etc.) protegido por um `Mutex` (vários
   clientes podem chamar `DarLance` ao mesmo tempo).
3. **Serialização:** cada chamada vira uma mensagem **Protocol Buffers**
   (binário, compacto) enviada sobre **HTTP/2**. É isso que faz o gRPC ser
   RPC de verdade: o cliente chama `client.dar_lance(...)` como se fosse
   uma função local, mas por trás isso serializa os dados, manda pela
   rede, e desserializa a resposta do outro lado.
4. **Streaming:** para `AcompanharLeilao`, o servidor usa um canal
   `tokio::sync::broadcast` interno — toda vez que chega um lance, o
   servidor publica no canal, e isso é repassado a todos os clientes que
   estão com o stream aberto.
5. **Encerramento automático:** o servidor derruba o leilão sozinho depois
   de `DURACAO_LEILAO_SEGUNDOS` (120s por padrão, dá pra mudar em
   `src/bin/servidor.rs`) e avisa todos via streaming.

## Rodando localmente (mesma máquina)

Pré-requisitos: Rust (`rustc`/`cargo`) e `protoc` (protobuf-compiler)
instalados.

**Instalar o `protoc` no Windows** (via [winget](https://learn.microsoft.com/pt-br/windows/package-manager/winget/)):

```powershell
winget install --id=Google.Protobuf -e
```

Depois, feche e abra o terminal de novo (para o PATH atualizar) e confira com:

```powershell
protoc --version
```

Se o `cargo build` ainda não achar o `protoc`, defina a variável de
ambiente `PROTOC` apontando para o executável, por exemplo:

```powershell
[Environment]::SetEnvironmentVariable('PROTOC', "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Google.Protobuf_Microsoft.Winget.Source_8wekyb3d8bbwe\bin\protoc.exe", 'User')
```

(No Linux, `sudo apt install protobuf-compiler`; no macOS, `brew install protobuf`.)

```bash
# Terminal 1 — servidor
cargo run --bin servidor

# Terminal 2 — cliente 1
cargo run --bin cliente -- Ana

# Terminal 3 — cliente 2
cargo run --bin cliente -- Bruno
```

No cliente, digite um número e Enter para dar um lance. Digite `sair`
para encerrar.

## Rodando em máquinas diferentes (requisito do trabalho)

1. Descubra o IP da máquina que vai rodar o **servidor** na rede local:
   ```bash
   ip a        # Linux
   ifconfig    # macOS
   ```
   Procure algo como `192.168.0.42`.

2. Nessa máquina, rode:
   ```bash
   cargo run --bin servidor
   ```
   (ele já escuta em `0.0.0.0:50051`, ou seja, em qualquer interface de
   rede da máquina, não só localhost).

3. Nas outras máquinas (os clientes), rode passando o IP do servidor:
   ```bash
   cargo run --bin cliente -- Ana http://192.168.0.42:50051
   cargo run --bin cliente -- Bruno http://192.168.0.42:50051
   ```

4. Se não conectar, verifique o firewall da máquina do servidor (a porta
   `50051/tcp` precisa estar liberada) e se todas as máquinas estão na
   mesma rede.

Alternativa se não tiver duas máquinas físicas disponíveis: usar duas
VMs, dois contêineres Docker na mesma rede, ou até o hotspot do celular
conectando notebook + celular.

## Testando o comportamento do leilão

- Dois clientes dando lances devem ver, em tempo real, o lance um do
  outro (via `AcompanharLeilao`), mesmo estando em máquinas diferentes.
- Um lance menor ou igual ao maior lance atual é recusado
  (`aceito: false`), mostrando validação no servidor.
- Depois de `DURACAO_LEILAO_SEGUNDOS`, o servidor encerra o leilão
  sozinho e todos os clientes recebem o evento `leilao_encerrado`.
