# Sistema Distribuido de Troca de Mensagens

**Instituicao:** Universidade do Estado de Santa Catarina - UDESC CCT  

**Professor:** Guilherme Piêgas  

**Mestrandos Responsáveis:** Luis Duarte e Guilherme Diel  

**Estudante:** Thais Faucz Jasse e Vinícius Correa Kock

**Disciplina:** Sistemas Distribuídos - Trabalho Final 2026/2

---

## 1. Resumo

Este projeto implementa um sistema distribuído de troca de mensagens entre usuários. A aplicação distribui usuários entre processos servidores independentes, cria novos servidores conforme a demanda e remove servidores ociosos quando a carga diminui.

O sistema mantem usuários e mensagens replicados entre os servidores, emprega identificadores único para garantir idempotencia e permite a eleição de um novo coordenador em caso de falha. Os usuários são simulados pelo componente Cadastro e a troca de mensagens ocorre por meio de sockets TCP.

## 2. Objetivos

- Distribuir usuários entre servidores com capacidade máxima de cinco usuários.
- Criar servidores automaticamente ao atingir a capacidade.
- Redistribuir usuários quando servidores falham ou são removidos.
- Manter réplicas consistentes do estado entre os servidores.
- Permitir o encaminhamento de mensagens entre servidores diferentes.
- Evitar o processamento duplicado de mensagens.
- Eleger um novo coordenador quando o coordenador atual falha.
- Registrar métricas em arquivos JSON Lines.

## 3. Arquitetura

O sistema possui os seguintes componentes:

### 3.1 Cadastro de usuários

O Cadastro não representa um usuário real. Ele fornece uma interface web para:

- cadastrar usuários;
- remover usuários;
- gerar usuários aleatórios;
- solicitar a remoção de servidores;
- iniciar envios automáticos;
- iniciar eleições;
- consultar o estado atual do sistema.

Arquivo principal:

```text
backend/cadastro.py
```

O Cadastro envia comandos ao coordenador por TCP e recebe notificações de controle na porta configurada em `CONTROL_PORT`.

### 3.2 Servidor do SD

Cada instância representa um processo servidor independente.

Arquivo principal:

```text
backend/servidor.py
```

Cada servidor pode armazenar no máximo cinco usuários. Os servidores:

- recebem usuários do coordenador;
- encaminham mensagens;
- replicam estado;
- enviam confirmações;
- detectam falhas;
- participam da eleição;
- redistribuem usuários;
- registram métricas.

### 3.3 Coordenador

O primeiro servidor iniciado atua como coordenador inicial. O coordenador:

- recebe os comandos `NEWUSER` e `KILL`;
- escolhe o servidor de cada usuário;
- encaminha mensagens;
- solicita a criação de servidores;
- dispara ou participa da eleição.

Quando o coordenador falha, o maior identificador entre os servidores vivos pode ser eleito como seu substituto.

## 4. Tecnologias e especificacoes

- Python 3.9 ou superior;
- biblioteca padrão do Python, sem instalacao de pacotes por `pip`;
- comunicacao por sockets TCP;
- interface web em HTML, CSS e JavaScript;
- Bootstrap pelo CDN;
- estado persistente em arquivos JSON;
- métricas em arquivos JSON Lines;
- sistema operacional Linux recomendado;
- terminal ou prompt de comandos;
- navegador moderno.

### 4.1 Protocolo de mensagens

Formatos utilizados pelo sistema:

```text
NEWUSER_<ID>
NEWUSER_<ID>_<NOME>
DELUSER_<ID>
KILL_<ID>
NEWID_<ID>
REMOVEDID_<ID>
NEWSERVERIP_<IP>_<PORT>
MSG_<MSG_ID>_<SENDER_ID>_<DEST_ID>_<CONTEUDO>
ACK_<MSG_ID>
ERROR_<DESCRICAO>
```

Exemplo:

```text
NEWUSER_12_Ana
MSG_17955481101420_1_3_Ola
ACK_17955481101420
```

Os campos devem ser separados pelo caractere `_`. `MSG_ID` deve ser numérico
(sem hífen) e permanece igual em todas as tentativas da mesma mensagem. Nomes e
conteúdos devem substituir underscores por espacos e não podem conter
caracteres de controle ou quebras de linha.

## 5. Estrutura do projeto

```text
trabalho final sdi/
|-- README.md
|-- .env.example
|-- .gitignore
|-- backend/
|   |-- cadastro.py
|   `-- servidor.py
`-- frontend/
    |-- index.html
    |-- script.js
    `-- style.css
```

Os arquivos `.env`, `cadastro.json`, `servidor_<ID>.json` e os logs
`*.jsonl` sao criados ou utilizados localmente e nao devem ser versionados.

## 6. Requisitos

Antes da execucao, confirme que:

1. Python 3.9 ou superior esta instalado;
2. as portas configuradas no arquivo `.env` estao disponiveis;
3. `SERVER_IP` e `SERVER_PORT` estão definidos no `.env` ou no ambiente;
4. não existe outro processo utilizando as portas configuradas;
5. o navegador pode acessar `http://localhost:8000`.

Para verificar o Python no Linux:

```bash
python3 --version
```

## 7. Como baixar o projeto

### 7.1 Por Git

```bash
git clone https://github.com/tataFaucz/TRABALHO_FINAL_SDI.git
cd TRABALHO_FINAL_SDI
```

O repositorio publico esta disponivel em:

<https://github.com/tataFaucz/TRABALHO_FINAL_SDI>

### 7.2 Sem Git

Baixe o projeto em formato ZIP, extraia os arquivos e abra um terminal dentro da pasta raiz do projeto.

Exemplo:

```bash
cd "/caminho/para/trabalho final sdi"
```

## 8. Configuracao

O arquivo `.env` deve estar na raiz do projeto, junto a `README.md`. Para gerar
a configuracao inicial a partir do modelo versionado, execute:

```bash
cp .env.example .env
```

O arquivo `.env` e ignorado pelo Git para evitar publicar configuracoes locais.
Edite-o antes de iniciar os componentes. O arquivo `.env.example` possui apenas
valores de exemplo; altere o `JOIN_TOKEN` antes de usar o sistema em uma rede.

Configuracao utilizada pelo projeto:

```env
SERVER_IP=127.0.0.1
PORTS=5001,5002,5003,5004,5005
SERVER_PORT=5001
ENCODING=utf-8

CONTROL_PORT=6001

HTTP_PORT=8000
HTTP_HOST=0.0.0.0

JOIN_TOKEN=alterar-este-token
MAX_USERS=5
HEARTBEAT_INTERVAL=2
FAILURE_TIMEOUT=5
```

### Variáveis obrigatorias

- `SERVER_IP`: endereço IP dos componentes;
- `SERVER_PORT`: porta obrigatória do servidor;
- `PORTS`: lista de portas para servidores adicionais (necessária para escala
  automática; opcional em uma execução de servidor único);
- `ENCODING`: codificação utilizada nas mensagens (opcional; padrão `utf-8`).

As variáveis de ambiente do sistema operacional possuem prioridade sobre as variáveis do arquivo `.env`.

## 9. Execucao

Cada componente deve ser iniciado em um terminal separado.

### 9.1 Iniciar o Cadastro

No diretorio raiz do projeto, execute:

```bash
python3 backend/cadastro.py
```

Saída esperada:

```text
Frontend: http://localhost:8000
Porta de controle: 6001
Coordenador inicial: 127.0.0.1:5001
```

### 9.2 Iniciar o coordenador

Em outro terminal:

```bash
python3 backend/servidor.py
```

Com as configuracoes padrão, esta instância usa a porta `5001` e o identificador `0`.

Se toda a rede estiver parada e o coordenador inicial tiver estado persistido,
confirme que nenhum outro nó está ativo e inicie-o como bootstrap local:

```bash
ALLOW_LOCAL_BOOTSTRAP=1 python3 backend/servidor.py
```

Essa opção só permite ao servidor inicial usar seu próprio estado quando não há
réplica disponível; os demais servidores continuam exigindo sincronização.
Não a use enquanto outro nó puder estar ativo.

Se o coordenador ativo marcou um servidor como desativado e você quiser
reativá-lo manualmente, solicite a operação ao coordenador:

```bash
python3 backend/servidor.py --reactivate 0
```

O comando valida o coordenador ativo, atualiza e replica o estado e solicita
que o coordenador inicie o servidor. Ele não apaga os arquivos de estado nem
inicia um segundo coordenador. Se `JOIN_TOKEN` estiver configurado, o comando
usa o token do `.env`. O processo iniciado fica sob controle do coordenador;
o comando retorna quando confirmar que a porta do servidor responde.
Se o coordenador já estava executando antes dessa funcionalidade ser instalada,
reinicie-o para que carregue o novo comando antes de solicitar a reativação.

### 9.3 Abrir o painel

Em um navegador moderno, acesse:

```text
http://localhost:8000
```

O painel permite adicionar usuários, iniciar o envio automatico, eliminar servidores e executar eleições.

## 10. Execucao de servidores adicionais

Os servidores devem ser executados em processos separados.

No terminal 3:

```bash
SERVER_ID=1 SERVER_PORT=5002 python3 backend/servidor.py
```

No terminal 4:

```bash
SERVER_ID=2 SERVER_PORT=5003 python3 backend/servidor.py
```

No terminal 5:

```bash
SERVER_ID=3 SERVER_PORT=5004 python3 backend/servidor.py
```

Cada processo recebe um `SERVER_PORT` diferente. O identificador deve corresponder a posicao da porta em `PORTS`.

| Posicao em `PORTS` | ID | Porta |
|---|---:|---:|
| 1 | 0 | 5001 |
| 2 | 1 | 5002 |
| 3 | 2 | 5003 |
| 4 | 3 | 5004 |
| 5 | 4 | 5005 |

O sistema tenta criar servidores adicionais automaticamente quando todos os servidores ativos estiverem cheios. Um servidor que retorna após falha sincroniza o estado e ingressa no anel antes de aceitar conexões.

## 11. Operacao pelo painel

### 11.1 Adicionar usuário

No formulario `Adicionar usuário`:

1. informe um identificador numerico maior que zero;
2. informe opcionalmente o nome;
3. clique em `Enviar NEWUSER`.

Exemplo de mensagem gerada:

```text
NEWUSER_12_Ana
```

### 11.2 Gerar usuários aleatórios

Informe uma quantidade entre 1 e 100 e clique em `Gerar e enviar`.

O Cadastro gera IDs livres e nomes aleatórios, mas o envio pode ser configurado para não incluir nomes.

### 11.3 Remover servidor

Informe o identificador do servidor e clique em `Enviar KILL`.

Exemplo:

```text
KILL_2
```

Somente os usuários associados ao servidor removido são redistribuídos entre os servidores restantes. Se um processo falhar sem um comando `KILL`, o coordenador detecta a falha, migra apenas os usuários daquele servidor e tenta reiniciá-lo após cinco segundos.

### 11.4 Remover usuário

Na lista de usuários, clique em `Remover` para excluir uma conta. O comando
`DELUSER_<ID>` atualiza o mapeamento e pode compactar os usuários nos servidores
restantes, desativando os recursos excedentes.

### 11.5 Envio automatico

O painel permite configurar:

- `T0`: intervalo inicial;
- `r`: fator de reducao, com `0 < r < 1`;
- `Tmin`: intervalo minimo;
- limite de usuários enviados.

O intervalo utilizado e:

```text
Tn = max(Tmin, T0 * r^n)
```

O usuário pode iniciar ou parar o envio automatico pelo painel.

Esse envio automático cria usuários. Separadamente, cada usuário ativo inicia
envios de mensagens para outro usuário em intervalos aleatórios de 0 a 10
segundos. São necessárias pelo menos duas contas para haver um destinatário.
O servidor do remetente encaminha `MSG` ao coordenador; o destinatário confirma
com `ACK_<MSG_ID>`. Se houver reenvio, o mesmo ID numérico é reutilizado.

### 11.6 eleição

Ao clicar em `Iniciar eleição`, o servidor iniciador utiliza o mecanismo de eleição em anel. O maior identificador dos servidores vivos torna-se o coordenador.

O painel apresenta:

- identificador do vencedor;
- ordem dos servidores no anel;
- percurso da mensagem `ELECTION`;
- anuncio de `COORDINATOR`.

## 12. Falha e recuperacao

### 12.1 Falha de servidor comum

O coordenador periodicamente verifica os servidores por meio de `PING`.

Quando detecta uma falha:

1. marca o tempo de deteccao;
2. redistribui os usuários;
3. atualiza o mapeamento;
4. replica o novo estado.

### 12.2 Falha do coordenador

Quando um servidor percebe que o coordenador não responde:

1. inicia uma eleição;
2. percorre o anel de servidores vivos;
3. escolhe o maior identificador;
4. propaga o novo coordenador;
5. atualiza o Cadastro por `NEWSERVERIP_<IP>_<PORT>`.

### 12.3 Retorno de servidor

Após uma falha detectada, o coordenador tenta reiniciar o processo após cinco
segundos. O servidor consulta os nós ativos, aplica a versão mais recente do
estado persistido e entra no anel antes de começar a atender operações.

O estado anterior não deve assumir a responsabilidade de coordenador sem antes reconhecer a configuracao atual.

## 13. Idempotencia

Cada mensagem possui um identificador unico no formato:

```text
MSG_<MSG_ID>_<SENDER_ID>_<DEST_ID>_<CONTEUDO>
```

O remetente pode reenviar uma mensagem caso não receba `ACK_<MSG_ID>` dentro do intervalo esperado.

O destinatário executa `receive_msg(conteudo)`: retorna `False` para `None` e,
caso contrário, aguarda um segundo e retorna `True`. Só após o processamento bem
sucedido o servidor persiste o conteúdo e o ID e confirma com ACK. Antes de
processar uma mensagem, consulta o registro persistente; se o ID já existir:

- `receive_msg` não sera executado novamente;
- o servidor reenvia o `ACK` correspondente;
- o resultado final permanece o mesmo.

O evento `receive_msg_execucoes_duplicadas` registra zero execuções extras;
`acks_reenviados_mensagens_processadas` contabiliza os ACKs repetidos.

## 14. Balanceamento

O servidor destinatario e escolhido pelo menor numero de usuários. Em caso de empate, o menor identificador entre os servidores vivos e utilizado.

Essa estrategia:

- evita concentrar muitos usuários em um unico servidor;
- distribui usuários de forma deterministica;
- permite calcular desbalanceamento e desvio padrão;
- minimiza mudancas desnecessarias na redistribuicao.

## 15. Persistencia e replicação

Cada servidor mantem um arquivo de estado no formato JSON:

```text
servidor_<ID>.json
```

Esse arquivo armazena:

- usuários;
- associacao usuário-servidor;
- identificadores de mensagens processadas;
- conteúdo das mensagens entregues;
- coordenador atual;
- epoca da configuracao.

O estado e replicado para os demais servidores por mensagens internas `SYNC`.

## 16. métricas

As métricas são registradas em arquivos JSON Lines:

```text
servidor_<ID>.jsonl
cadastro.jsonl
```

Cada linha possui o formato:

```json
{"ts":1730000000.123,"componente":"coordenador","id":0,"metrica":"tempo_deteccao_falha","valor":5.12}
```

As métricas incluem:

- desbalanceamento;
- desvio padrão de usuários;
- usuários migrados;
- servidores ativos;
- tempo de criação de servidor;
- `tempo_desativacao_servidor`: duração observada até o servidor parar de responder;
- `tempo_deteccao_falha`: duração observada entre o último heartbeat e a detecção;
- `tempo_redistribuicao`: duração real da migração dos usuários afetados;
- tempo de eleição;
- atraso de replicação;
- `tempo_sincronizacao_retorno`: duração da sincronização na inicialização/retorno;
- `receive_msg_execucoes_duplicadas`: execuções extras de `receive_msg` (valor esperado: zero);
- mensagens perdidas;
- mensagens confirmadas;
- `mensagens_enviadas` e `reenvios_mensagens` atribuídos ao usuário remetente;
- `acks_reenviados_mensagens_processadas`: ACKs reenviados para entregas já processadas;
- usuários sem servidor responsavel.

Os registros de usuário usam `componente="usuario"` e o ID do usuário; os
registros de coordenação usam `componente="coordenador"`. As linhas são
armazenadas no arquivo JSONL do servidor que executa a operação.

## 17. Testes sugeridos

### 17.1 Capacidade e escala

1. Inicie o Cadastro e o servidor 0.
2. Cadastre seis usuários.
3. Confirme que o servidor 1 foi criado.
4. Confirme que nenhum servidor possui mais de cinco usuários.

### 17.2 Mensagens entre servidores

1. Distribua usuários entre dois servidores.
2. Envie uma mensagem de um usuário para outro.
3. Confirme o recebimento de `ACK_<MSG_ID>`.

### 17.3 Idempotencia

1. Envie uma mensagem.
2. Reenvie a mesma mensagem com o mesmo `MSG_ID`.
3. Confirme que `receive_msg` foi executado apenas uma vez.
4. Confirme o reenvio do ACK.

### 17.4 Falha de servidor

1. Remova um servidor pelo painel ou encerre seu processo.
2. Aguarde a deteccao da falha.
3. Verifique a redistribuicao dos usuários.
4. Confirme a ausencia de usuários sem servidor.

### 17.5 Falha do coordenador

1. Encerre o processo do servidor coordenador.
2. Aguarde a eleição.
3. Confirme que o maior identificador vivo foi eleito.
4. Confirme a atualizacao do coordenador no Cadastro.
5. Reinicie o coordenador anterior e verifique se ele sincroniza antes de atuar.

### 17.6 Reducao de recursos

1. Remova ou redistribua usuários ate apenas um servidor ser necessario.
2. Confirme a desativacao dos servidores ociosos excedentes.
3. Verifique as métricas de desativacao e escalabilidade.

## 18. Solucao de problemas

### Porta ja esta em uso

Verifique os processos associados:

```bash
sudo lsof -i :5001
sudo lsof -i :8000
```

Finalize somente o processo que não pertence ao projeto ou altere a configuracao no `.env`.

### Cadastro não abre

Confirme:

```bash
curl http://localhost:8000/api/status
```

Se houver erro de conexao, verifique se `cadastro.py` esta em execucao e se `HTTP_PORT=8000`.

### Servidor não responde

Teste a porta diretamente:

```bash
nc -v 127.0.0.1 5001
```

Pressione `Ctrl+C` caso o comando fique aberto.

### não foi possivel criar um novo servidor

Confirme:

- a porta correspondente existe em `PORTS`;
- não existe outro processo utilizando essa porta;
- o `SERVER_ID` corresponde ao indice correto;
- o Python possui permissão para iniciar um novo processo.

### Coordenador não atualiza

Confirme:

- o servidor pode acessar `CONTROL_PORT=6001`;
- o processo de Cadastro esta ativo;
- o servidor recebeu `NEWSERVERIP_<IP>_<PORT>`.

## 19. Encerramento

Para encerrar um componente, utilize `Ctrl+C` no terminal correspondente.

Os estados persistidos são mantidos em arquivos JSON. Para reiniciar os dados de testes:

```bash
rm -f servidor_*.json servidor_*.jsonl cadastro.json cadastro.jsonl
```

Esse comando apaga os estados e logs persistidos. Ele não apaga o codigo-fonte.

## 20. Consideracoes finais

O projeto combina distribuicao dinâmica, tolerância à falhas, replicação de estado, idempotencia e eleição em anel. A separação em processos independentes permite testar falhas de componentes sem interromper necessariamente todo o sistema.
