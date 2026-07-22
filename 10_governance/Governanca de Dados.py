# Databricks notebook source
# DBTITLE 1,Governança de Dados no Unity Catalog
# MAGIC %md
# MAGIC # Governança de Dados 🛡️
# MAGIC
# MAGIC ## Objetivo
# MAGIC Demonstrar os mecanismos de governança que o **Unity Catalog** oferece nativamente,
# MAGIC aplicados às tabelas reais deste projeto: documentação de catálogo, classificação
# MAGIC de dados sensíveis, masking/row filter e controle de acesso.
# MAGIC
# MAGIC ## Um recorte importante: de quem é essa responsabilidade?
# MAGIC Em times de dados maduros, isso normalmente é responsabilidade do **time de
# MAGIC Engenharia de Dados / Plataforma / Governança**, não do Data Scientist —
# MAGIC quem administra `GRANT`/`REVOKE`, tags de classificação e masking em produção
# MAGIC costuma ser quem também administra o catálogo.
# MAGIC
# MAGIC O papel real do Data Scientist nesse assunto é outro:
# MAGIC - **Saber identificar** qual coluna é sensível e **pedir** a classificação certa
# MAGIC   (é o cientista de dados que sabe que `age`/`gender`/`country` carregam dado
# MAGIC   pessoal, mesmo que não seja ele quem aplica a tag em produção)
# MAGIC - **Desenhar modelos assumindo** que o dado sensível pode estar mascarado/filtrado
# MAGIC - Manter a **governança do lado do modelo** (explicabilidade, versionamento,
# MAGIC   critério de promoção) — isso sim é terreno direto do Data Scientist, e já está
# MAGIC   coberto em `Model_Explainability_SHAP.py` e `Automated Model Retraining.py`
# MAGIC
# MAGIC Este notebook existe para mostrar que essa fronteira é conhecida e respeitada —
# MAGIC não para simular que o papel de engenharia de plataforma foi assumido por engano.

# COMMAND ----------

# DBTITLE 1,Configuração
CATALOG = "customer_intelligence"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

print("✓ Configuração carregada")
print(f"  Catalog: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,1. Documentar o Catálogo (COMMENT ON)
# MAGIC %md
# MAGIC ## 1. Documentação do Catálogo
# MAGIC
# MAGIC Governança inclui documentação, não só segurança: `COMMENT ON` deixa a
# MAGIC descrição de tabelas/colunas visível direto no Catalog Explorer, pra quem
# MAGIC nunca abriu o código do notebook entender o que cada tabela significa.

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA_SILVER}.customers IS
    'Clientes após limpeza/conformação (camada Silver). Fonte: {CATALOG}.bronze.customers_raw.
    Contém dado pessoal (age, gender, country) — ver seção de classificação abaixo.'
""")

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA_GOLD}.customer_features IS
    'Feature store de clientes (RFM + comportamento + campanhas) usada para treinar
    os modelos de churn e propensity. Uma linha por customer_id.'
""")

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA_GOLD}.churn_labels IS
    'Target de churn (churn_label) derivado de is_churned + regra de recência.
    Ver 03_gold/Feature Engineering Gold.py para a lógica exata.'
""")

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA_GOLD}.churn_predictions IS
    'Predições do modelo de churn no conjunto de teste (actual_churn vs predicted_churn),
    usada por Automated Model Retraining.py para medir performance em "produção".'
""")

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA_GOLD}.customer_scores IS
    'Scores gerados em lote (churn, customer value, engagement) por 05_scoring/Batch Scoring.py.'
""")

print("✓ Tabelas documentadas via COMMENT ON")

# COMMAND ----------

# DBTITLE 1,Documentar colunas sensíveis
spark.sql(f"""
    COMMENT ON COLUMN {CATALOG}.{SCHEMA_SILVER}.customers.customer_id IS
    'Identificador pseudo-anonimizado do cliente (não é PII direto, mas é a chave
    que liga todo o histórico da pessoa entre as tabelas — tratar como sensível).'
""")

spark.sql(f"""
    COMMENT ON COLUMN {CATALOG}.{SCHEMA_SILVER}.customers.age IS
    'Dado pessoal (LGPD/GDPR). Idade exata do cliente.'
""")

spark.sql(f"""
    COMMENT ON COLUMN {CATALOG}.{SCHEMA_SILVER}.customers.gender IS
    'Dado pessoal (LGPD/GDPR), potencialmente sensível dependendo da jurisdição.'
""")

print("✓ Colunas sensíveis documentadas")

# COMMAND ----------

# DBTITLE 1,2. Classificar Dados Sensíveis (Tags)
# MAGIC %md
# MAGIC ## 2. Classificação de Dados Sensíveis
# MAGIC
# MAGIC Este dataset sintético não tem email/telefone/CPF — mas **tem dado pessoal
# MAGIC de verdade**: `age`, `gender` e `country` são dados pessoais sob LGPD/GDPR,
# MAGIC mesmo sem serem diretamente identificadores. `customer_id`, embora já
# MAGIC pseudo-anonimizado, é a chave que liga tudo — também merece classificação.
# MAGIC
# MAGIC O Unity Catalog suporta tags key-value em nível de coluna, que ficam visíveis
# MAGIC no Catalog Explorer e podem ser usadas por ferramentas de compliance para
# MAGIC descobrir automaticamente onde o dado sensível está.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_SILVER}.customers
    ALTER COLUMN customer_id
    SET TAGS ('data_classification' = 'pseudo_identifier')
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_SILVER}.customers
    ALTER COLUMN age
    SET TAGS ('data_classification' = 'personal_data', 'regulation' = 'lgpd_gdpr')
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_SILVER}.customers
    ALTER COLUMN gender
    SET TAGS ('data_classification' = 'personal_data', 'regulation' = 'lgpd_gdpr')
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_SILVER}.customers
    ALTER COLUMN country
    SET TAGS ('data_classification' = 'personal_data', 'regulation' = 'lgpd_gdpr')
""")

print("✓ Tags de classificação aplicadas em silver.customers")
print("  Confira em Catalog Explorer > customer_intelligence > silver > customers > Columns")

# COMMAND ----------

# DBTITLE 1,3. Column Masking e Row Filter (tabela de demonstração isolada)
# MAGIC %md
# MAGIC ## 3. Masking e Row Filter
# MAGIC
# MAGIC Masking/row filter mudam o **resultado da query**, não só metadado — por
# MAGIC isso, em vez de aplicar em `silver.customers` (que alimenta todo o resto do
# MAGIC pipeline: Silver → Gold → modelos), criamos uma **tabela de demonstração
# MAGIC isolada**. Aplicar isso nas tabelas de produção do projeto arriscaria mudar
# MAGIC silenciosamente os dados que os outros notebooks já validaram.

# COMMAND ----------

df_demo = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.customers").select(
    "customer_id", "age", "gender", "country"
)
df_demo.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA_GOLD}.customers_governance_demo"
)
print(f"✓ Tabela de demonstração criada: {CATALOG}.{SCHEMA_GOLD}.customers_governance_demo")

# COMMAND ----------

# DBTITLE 1,Column Mask: expor apenas a década de idade
# is_account_group_member('account users') é praticamente sempre verdadeiro para
# quem está logado no workspace — então, sem um grupo restrito de verdade
# configurado, essa máscara mascara para TODO MUNDO (inclusive você). Isso é
# esperado: é só pra demonstrar o mecanismo, não simular uma política real
# de acesso (que precisaria de um grupo dedicado, ex: 'governance_readers').
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_GOLD}.mask_age(age INT)
    RETURNS INT
    RETURN CASE
        WHEN is_account_group_member('admins') THEN age
        ELSE (age DIV 10) * 10
    END
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_GOLD}.customers_governance_demo
    ALTER COLUMN age
    SET MASK {CATALOG}.{SCHEMA_GOLD}.mask_age
""")

print("✓ Column mask aplicado em customers_governance_demo.age")
print("\nAntes de mascarar (silver.customers, sem mask):")
spark.table(f"{CATALOG}.{SCHEMA_SILVER}.customers").select("customer_id", "age").show(5)

print("Depois de mascarar (customers_governance_demo, com mask — 'admins' vê o real, resto vê arredondado):")
spark.table(f"{CATALOG}.{SCHEMA_GOLD}.customers_governance_demo").select("customer_id", "age").show(5)

# COMMAND ----------

# DBTITLE 1,Row Filter: restringir visibilidade por país
# Mesmo racional do mask acima: sem um grupo dedicado configurado, isso filtra
# pra todo mundo que não é 'admins'. O objetivo aqui é mostrar a sintaxe e o
# efeito, não simular uma política de multi-tenant real.
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_GOLD}.filter_by_country(country STRING)
    RETURNS BOOLEAN
    RETURN is_account_group_member('admins') OR country = 'BR'
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_GOLD}.customers_governance_demo
    SET ROW FILTER {CATALOG}.{SCHEMA_GOLD}.filter_by_country ON (country)
""")

print("✓ Row filter aplicado em customers_governance_demo (só linhas country='BR' visíveis para não-admins)")

# COMMAND ----------

# DBTITLE 1,Reverter mask/filter (deixar a tabela de demo limpa)
# Reversão explícita — governança inclui saber desfazer um controle sem
# precisar recriar a tabela do zero.
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_GOLD}.customers_governance_demo
    ALTER COLUMN age DROP MASK
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA_GOLD}.customers_governance_demo
    DROP ROW FILTER
""")

print("✓ Mask e row filter revertidos em customers_governance_demo (fica só como exemplo de sintaxe)")

# COMMAND ----------

# DBTITLE 1,4. GRANT / REVOKE
# MAGIC %md
# MAGIC ## 4. Controle de Acesso (GRANT/REVOKE)
# MAGIC
# MAGIC Sintaxe padrão de concessão de acesso no Unity Catalog — testado aqui na
# MAGIC tabela de demonstração, nunca nas tabelas de produção do projeto.
# MAGIC `` `account users` `` é o grupo embutido que cobre todo mundo com acesso
# MAGIC ao workspace; em produção, o ideal é conceder a grupos específicos
# MAGIC (ex: `data_science_team`), não a esse grupo amplo.

# COMMAND ----------

spark.sql(f"""
    GRANT SELECT ON TABLE {CATALOG}.{SCHEMA_GOLD}.customers_governance_demo
    TO `account users`
""")
print("✓ SELECT concedido a `account users` em customers_governance_demo")

spark.sql(f"""
    REVOKE SELECT ON TABLE {CATALOG}.{SCHEMA_GOLD}.customers_governance_demo
    FROM `account users`
""")
print("✓ SELECT revogado de `account users` em customers_governance_demo (demonstração completa)")

# COMMAND ----------

# DBTITLE 1,Resumo
# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Mecanismo | Aplicado onde | Reversível? |
# MAGIC |---|---|---|
# MAGIC | `COMMENT ON TABLE/COLUMN` | Tabelas de produção (silver.customers, gold.*) | Sim (comentário pode ser substituído) |
# MAGIC | Tags de classificação (`SET TAGS`) | Colunas reais de silver.customers | Sim (`UNSET TAGS`) |
# MAGIC | Column Mask | Tabela de demonstração isolada | Sim (já revertido acima) |
# MAGIC | Row Filter | Tabela de demonstração isolada | Sim (já revertido acima) |
# MAGIC | `GRANT`/`REVOKE` | Tabela de demonstração isolada | Sim (já revogado acima) |
# MAGIC
# MAGIC **Posicionamento para a entrevista**: isso demonstra consciência dos
# MAGIC mecanismos de governança do Unity Catalog — não uma reivindicação de que
# MAGIC esse é o trabalho do dia a dia de um Data Scientist. A governança que é
# MAGIC genuinamente minha, como cientista de dados, está do lado do modelo:
# MAGIC explicabilidade (SHAP), versionamento controlado (Champion/Challenger) e
# MAGIC critério objetivo de promoção — ver `Model_Explainability_SHAP.py` e
# MAGIC `Automated Model Retraining.py`.

# COMMAND ----------
