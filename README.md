# Market Data Pipeline 📊

Este repositório contém um pipeline automatizado de web scraping para extração de preços de varejo, desenvolvido em Python. O projeto foi refatorado de uma arquitetura baseada em nuvem (GCP/Airflow) para um ambiente local conteinerizado.

## 🏗️ Arquitetura e Tecnologias

* **Linguagem:** Python 3 (Requests, BeautifulSoup, Pandas, SQLAlchemy)
* **Banco de Dados:** PostgreSQL 16 (Local)
* **Infraestrutura:** Docker & Docker Compose
* **Orquestração/Automação:** GitHub Actions (Execução diária)
* **Estratégia de Dados:** Particionamento nativo no PostgreSQL utilizando a chave `DATA_REF`.

## ⚙️ Pré-requisitos

Para rodar este projeto localmente, você precisará ter instalado:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (ou Docker Engine)
* [Git](https://git-scm.com/)

## 🚀 Como executar o projeto

**1. Clone o repositório**
```bash
git clone [https://github.com/dmspinto93/market_data.git](https://github.com/dmspinto93/market_data.git)
cd market_data