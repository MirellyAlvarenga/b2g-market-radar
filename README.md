# B2G Market Radar — Inteligência Comercial em Licitações Públicas

Plataforma ponta a ponta de Engenharia e Análise de Dados que transforma dados abertos do **Portal Nacional de Contratações Públicas (PNCP)** em inteligência competitiva para empresas que vendem para o setor público (B2G).

## 1. Problema de Negócio
Empresas privadas que participam de licitações enfrentam baixa visibilidade sobre onde está a demanda, qual a agressividade de preços dos concorrentes e quais órgãos concentram orçamento. Este produto responde a 4 perguntas estratégicas:

1. **Tamanho de Mercado (TAM):** Qual o volume financeiro contratado por UF, esfera (Federal, Estadual, Municipal) e modalidade?
2. **Competitividade e Pricing:** Qual a taxa média de desconto (deságio) entre o valor estimado pelo órgão e o valor homologado?
3. **Análise de Concorrência:** Quais empresas (CNPJs) detêm o maior *market share* e ticket médio por segmento?
4. **Sazonalidade e Pipeline:** Como os gastos se distribuem ao longo dos meses e qual o *lead time* médio de homologação?

## 2. Arquitetura (Medallion)
- **Fonte:** API de Consulta do PNCP (`/v1/contratacoes/publicacao` e `/v1/contratos`) + API de Localidades do IBGE.
- **Ingestão:** Python (`httpx`, `pandas`/`polars`, `sqlalchemy`) com tratamento de paginação, *rate limit* e logs estruturados.
- **Armazenamento & Transformação:** PostgreSQL (via Docker) + **dbt Core** (`bronze` -> `silver` -> `gold` em Star Schema).
- **Qualidade & CI:** Testes de schema e integridade no dbt executados via GitHub Actions.
- **Visualização:** Dashboard executivo no Power BI conectado diretamente à camada `gold`.

## 3. Roadmap de Evolução
- [ ] **Fase 1 (MVP):** Infraestrutura Docker + ingestão de 1 mês de contratações do PNCP na camada Bronze.
- [ ] **Fase 2 (Modelagem):** Camadas Silver e Gold (Star Schema) no dbt + testes automatizados + Dashboard v1 no Power BI.
- [ ] **Fase 3 (Enriquecimento):** Cruzamento com API do IBGE e BrasilAPI (porte e CNAE dos fornecedores).
- [ ] **Fase 4 (Observabilidade):** Monitor de qualidade de dados com alertas via Webhook.
