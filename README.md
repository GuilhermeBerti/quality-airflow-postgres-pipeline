# 📊 Data Quality Pipeline com Airflow e PostgreSQL

## 🚀 Visão Geral
Este projeto implementa uma **pipeline de monitoramento de qualidade de dados** utilizando Apache Airflow e PostgreSQL.

O objetivo é validar automaticamente a qualidade dos dados carregados em uma tabela de staging, com base em dimensões clássicas de Data Quality, garantindo maior confiabilidade para análises e tomada de decisão.

---

## 🧠 Conceitos Aplicados
- **Completude** → Dados obrigatórios preenchidos  
- **Unicidade** → Ausência de duplicidades  
- **Validade** → Formato correto dos dados  
- **Consistência** → Regras de negócio respeitadas  
- **Atualidade (Freshness)** → Dados atualizados  
- **Acurácia** → Valores corretos e plausíveis  

---

## 🏗️ Arquitetura
CSV / Parquet → Airflow DAG → PostgreSQL (staging) → Data Quality Checks → data_quality_logs

---

## ⚙️ Tecnologias
- Apache Airflow  
- Python  
- PostgreSQL  
- Pandas  
- SQL  

---

## 🔄 Fluxo da Pipeline
1. Criação das tabelas  
2. Ingestão de dados (CSV/Parquet)  
3. Execução dos checks  
4. Persistência dos logs  
5. Monitoramento sem quebra da DAG  

---

## 📋 Estrutura das Tabelas

### staging_data
- id (INT)
- nome (TEXT)
- cpf (TEXT)
- email (TEXT)
- preco (FLOAT)
- data_pedido (TIMESTAMP)
- data_entrega (TIMESTAMP)
- data_atualizacao (TIMESTAMP)

### data_quality_logs
- id (SERIAL)
- check_name (VARCHAR)
- status (VARCHAR)
- execution_date (TIMESTAMP)
- error_message (TEXT)

---

## 🧪 Checks Implementados

### Completude
SELECT COUNT(*) FROM staging_data WHERE email IS NULL;

### Unicidade
SELECT cpf FROM staging_data GROUP BY cpf HAVING COUNT(*) > 1;

### Validade
SELECT COUNT(*) FROM staging_data WHERE email IS NOT NULL AND email NOT LIKE '%@%.%';

### Consistência
SELECT COUNT(*) FROM staging_data WHERE data_entrega < data_pedido;

### Atualidade
SELECT MAX(data_atualizacao) FROM staging_data;

### Acurácia
SELECT COUNT(*) FROM staging_data WHERE preco < 0;

---

## 📊 Exemplo de Resultado

- completeness → FAIL (emails nulos)
- uniqueness → FAIL (CPF duplicado)
- validity → FAIL (emails inválidos)
- consistency → FAIL (datas inconsistentes)
- freshness → SUCCESS
- accuracy → FAIL (preços negativos)

---

## 📈 Métrica de Qualidade

SELECT 
    COUNT(*) FILTER (WHERE status = 'SUCCESS') * 100.0 / COUNT(*) AS quality_score
FROM data_quality_logs;

---

## 🎯 Diferenciais
- Pipeline automatizada com Airflow  
- Uso de COPY (alta performance)  
- Logs persistentes para auditoria  
- Separação de ingestão e validação  
- Estrutura escalável  

---

## 🧠 Decisões
- Monitoramento sem quebrar pipeline  
- Foco em observabilidade  
- Histórico de qualidade  

---

## 🚀 Evoluções Futuras
- Dashboard (Streamlit / Power BI)  

---

## ▶️ Como Executar
1. Subir Airflow  
2. Configurar conexão Postgres  
3. Colocar dados em /data  
4. Ativar DAG  
5. Executar  

---

## 💥 Conclusão
Pipeline de qualidade de dados automatizada, com foco em monitoramento, rastreabilidade e melhoria contínua.
