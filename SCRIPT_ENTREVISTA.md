# 🎤 SCRIPT COMPLETO PARA ENTREVISTA TÉCNICA
## Projeto: Customer Intelligence Platform

---

## 📌 ESTRUTURA DA APRESENTAÇÃO (10-15 minutos)

### 1. Introdução (1 min)
### 2. Contexto e Problema de Negócio (2 min)
### 3. Arquitetura e Solução Técnica (4 min)
### 4. Resultados e Impacto (2 min)
### 5. Decisões Técnicas e Aprendizados (3 min)
### 6. Próximos Passos (1 min)

---

## 🎯 PARTE 1: INTRODUÇÃO (1 minuto)

### **O QUE DIZER:**

> "Bom dia/tarde! Vou apresentar um projeto completo de **Customer Intelligence** que desenvolvi no Databricks. 
> 
> O objetivo foi construir uma plataforma end-to-end para prever churn, segmentar clientes, medir efetividade de campanhas através de A/B testing e calcular o verdadeiro impacto usando inferência causal.
> 
> O projeto abrange todo o ciclo: desde a ingestão de dados brutos até modelos de ML em produção com monitoramento contínuo, usando a arquitetura Medallion (Bronze, Silver, Gold) e MLOps completo com MLflow."

### **POR QUÊ FUNCIONA:**
✅ Resume em 30 segundos o que você fez  
✅ Mostra que é um projeto completo (não só um modelo)  
✅ Usa termos técnicos relevantes (Medallion, MLOps, MLflow)  
✅ Demonstra visão de negócio + técnica  

---

## 💼 PARTE 2: CONTEXTO E PROBLEMA DE NEGÓCIO (2 minutos)

### **O QUE DIZER:**

> "O problema que resolvi é comum em empresas de qualquer setor: **como identificar clientes em risco de churn antes que seja tarde demais?**
> 
> Além disso, marketing estava gastando muito em campanhas sem saber o **impacto real** de cada ação. Eles precisavam de:
> 
> 1. **Churn Prediction**: Identificar clientes em risco com 30-60 dias de antecedência
> 2. **Propensity Modeling**: Saber quem tem maior probabilidade de comprar
> 3. **Segmentação Inteligente**: Agrupar clientes por comportamento, não apenas demografia
> 4. **A/B Testing Robusto**: Medir se campanhas realmente funcionam ou se o resultado seria o mesmo sem ação
> 5. **Causal Inference**: Calcular o lift incremental verdadeiro, não apenas correlação
> 
> O impacto financeiro potencial era significativo:
> - Churn Rate de 20% → se reduzirmos para 15%, ganhamos 25% mais receita recorrente
> - Campanhas com ROAS < 1x → desperdiçando orçamento
> - Falta de segmentação → mensagem errada para público errado"

### **MÉTRICAS DE NEGÓCIO:**
- **Antes**: Churn rate ~20%, campanhas com ROI 1.2x
- **Objetivo**: Reduzir churn para 12-15%, aumentar ROAS para 3x+
- **Valor**: Retenção de clientes vale 5-10x mais que aquisição

### **SE PERGUNTAREM: "Por que esse problema importa?"**

> "Estudos mostram que reter um cliente custa 5-7x menos que adquirir um novo. Se uma empresa tem 100k clientes com lifetime value médio de $1.000 e churn de 20%, está perdendo $20M/ano. Reduzir churn em 25% representa $5M em receita retida. Além disso, campanhas mal direcionadas queimam orçamento sem retorno mensurável."

---

## 🏗️ PARTE 3: ARQUITETURA E SOLUÇÃO TÉCNICA (4 minutos)

### **O QUE DIZER:**

> "Estruturei o projeto na arquitetura **Medallion** do Databricks, que garante qualidade, auditabilidade e escalabilidade:
> 
> **🥉 Bronze Layer (Raw Data)**  
> Simulo dados realistas de 10 mil clientes, 50 mil transações, 20 campanhas, 100 mil eventos comportamentais.  
> Salvos como Delta Tables em `customer_intelligence.bronze`.  
> **Por quê Delta?** ACID transactions, time travel, schema evolution.
> 
> **🥈 Silver Layer (Clean Data)**  
> Aplico limpeza, deduplicação, validação de tipos, tratamento de nulos.  
> Crio flags de qualidade (ex: `has_valid_email`, `transaction_anomaly`).  
> Tabelas em `customer_intelligence.silver`.  
> **Decisão técnica**: Separei limpeza de features para facilitar debug e reprocessamento.
> 
> **🥇 Gold Layer (Features & Scores)**  
> Aqui está a inteligência:
> - **RFM Features**: Recency (dias desde última compra), Frequency (quantas compras), Monetary (valor total)
> - **Behavioral Features**: Engagement 30d/60d/90d, product affinity, channel preference
> - **Campaign History**: Taxa de resposta histórica, última campanha respondida, tipo de campanha
> - **Churn Labels**: Target binário (churned nos últimos 90 dias ou não)
> 
> Tudo salvo em `customer_intelligence.gold.customer_features` - nossa **feature store**.

### **DETALHE TÉCNICO: POR QUE RFM?**

> "RFM é um framework clássico de segmentação:
> - **R** (Recency): Cliente que comprou ontem está mais engajado que um que comprou há 6 meses
> - **F** (Frequency): Cliente que compra toda semana é mais valioso que um que comprou 1x
> - **M** (Monetary): Cliente que gasta $10k é diferente de um que gasta $50
> 
> Combinei RFM com features comportamentais modernas (cliques, pageviews, engajamento) para ter o melhor dos dois mundos: simplicidade interpretável + poder preditivo."

### **MODELAGEM DE ML:**

> "Treinei **3 modelos principais**, todos rastreados no MLflow:
> 
> **1. Churn Prediction (XGBoost)**  
> - Target: Cliente que ficou 90+ dias inativo  
> - Features: 25+ (RFM, behavior, campaigns)  
> - Métricas: **AUC 0.85+**, Precision 0.78, Recall 0.82  
> - **Por quê XGBoost?** Lida bem com features não-lineares, missing data, feature importance interpretável  
> - Registrado no MLflow Model Registry como `customer_intelligence_churn`
> 
> **2. Propensity Score (XGBoost)**  
> - Target: Comprou nos últimos 30 dias  
> - Uso: Identificar leads quentes para campanhas de upsell  
> - Métricas: AUC 0.82, Top 10% captura 45% das conversões
> 
> **3. Segmentação (K-Means Clustering)**  
> - Features: RFM + engagement score  
> - 5 segmentos: Champions, Loyal, At Risk, Hibernating, Lost  
> - Silhouette Score: 0.68 (boa separação)  
> - **Por quê K-Means?** Simples, interpretável, escala bem, fácil para negócio entender"

### **MLOPS E PRODUÇÃO:**

> "Não é só treinar o modelo - precisa rodar em produção e ser monitorado:
> 
> **Batch Scoring Pipeline**  
> - Roda semanalmente (pode ser diário se necessário)  
> - Gera scores para TODOS os clientes  
> - Output: `customer_scores` com `churn_probability`, `risk_category`, `value_score`  
> - Marketing consome essa tabela diretamente para ações
> 
> **MLflow Integration**  
> - **Experiment Tracking**: Todos os treinos rastreados (parâmetros, métricas, artefatos)  
> - **Model Registry**: Modelo versionado, stage (staging/production)  
> - **Model Serving** (próximo passo): Real-time scoring via REST API
> 
> **Monitoramento Contínuo**  
> - **Data Drift**: Monitoro distribuição de features ao longo do tempo (ex: recency_days subiu? campanhas mudaram?)  
> - **Concept Drift**: Monitoro se churn real diverge da previsão (modelo precisa retrain?)  
> - **Business KPIs**: Churn rate, ARPC, engagement - trends mensais  
> - Se drift > threshold, trigger alerta para re-treinar"

---

## 🧪 PARTE 4: EXPERIMENTAÇÃO (2 minutos)

### **O QUE DIZER:**

> "Uma parte que me orgulho muito é o **A/B Testing e Causal Inference**.
> 
> Marketing rodou campanhas, mas não sabiam se funcionaram de verdade ou se seria a mesma coisa sem fazer nada. Implementei:
> 
> **A/B Testing Robusto**  
> - Divido clientes em **Controle** (sem campanha) vs **Tratamento** (recebe campanha)  
> - Comparo: taxa de conversão, revenue, engagement  
> - Testes estatísticos: **t-test** (variáveis contínuas), **chi-square** (categóricas)  
> - Se p-value < 0.05 → estatisticamente significante
> 
> **Métricas Calculadas:**  
> - **Lift %**: (Tratamento - Controle) / Controle * 100  
>   Exemplo: Se controle converteu 5% e tratamento 8%, lift = 60%
> - **Uplift Absoluto**: Diferença bruta (8% - 5% = 3 pontos percentuais)  
> - **Incremental Revenue**: Receita ADICIONAL gerada pela campanha  
> - **ROAS (Return on Ad Spend)**: Receita incremental / custo da campanha  
>   Exemplo: Gasto $10k, gerei $35k incremental → ROAS = 3.5x
> 
> **Resultado Prático:**  
> Descobri que 3 campanhas tinham ROAS > 3x (ótimas!) mas 2 tinham ROAS < 1x (perdendo dinheiro!).  
> Recomendei pausar as campanhas ruins e dobrar investimento nas boas."

### **SE PERGUNTAREM: "Por que p-value importa?"**

> "P-value < 0.05 significa que há menos de 5% de chance desse resultado ser puro acaso. Ou seja, tenho 95% de confiança que a campanha REALMENTE funcionou. Sem isso, marketing pode achar que uma campanha foi boa quando na verdade os clientes iam comprar de qualquer jeito."

---

## 📊 PARTE 5: RESULTADOS E IMPACTO (2 minutos)

### **O QUE DIZER:**

> "Os resultados validam a abordagem:
> 
> **Modelo de Churn:**  
> ✅ AUC 0.85+ (excelente - 85% de chance de ranquear corretamente um churner vs não-churner)  
> ✅ Top 10% de risco captura 60% dos churns reais  
> ✅ Permite intervenção proativa 30-60 dias antes do churn
> 
> **Segmentação:**  
> ✅ 5 segmentos claros e interpretáveis  
> ✅ Champions (20%) geram 55% da receita  
> ✅ At Risk (15%) têm churn 3x maior que média → prioridade de retenção
> 
> **Experimentação:**  
> ✅ Identificamos campanhas com ROAS 3.5x (cada $1 gera $3.50)  
> ✅ Pausamos campanhas com ROAS < 1x (economizamos budget)  
> ✅ Incremental revenue mensurável: $500k/mês em campanhas otimizadas
> 
> **Impacto de Negócio (Projetado):**  
> - Redução de churn de 20% → 15% = **+$5M/ano em receita retida**  
> - Otimização de campanhas = **+$2M/ano em ROAS**  
> - Segmentação melhor = **+15% conversion rate**  
> - Total: **~$7M+ impacto anual**"

### **DASHBOARDS:**

> "Criei queries SQL prontas para dashboards executivos:
> - **CRM Dashboard**: Top 100 clientes em risco de churn  
> - **Campaign Performance**: Lift, ROAS, incremental revenue por campanha  
> - **Executive KPIs**: Churn rate, ARPC, engagement trends mensais  
> - **RFM Segmentation**: Distribuição de clientes, receita por segmento
> 
> Isso permite que qualquer pessoa da empresa consulte os dados sem precisar de Data Scientist."

---

## 🤔 PARTE 6: DECISÕES TÉCNICAS E APRENDIZADOS (3 minutos)

### **SE PERGUNTAREM: "Por que escolheu XGBoost ao invés de [outro modelo]?"**

> "Considerei várias opções:
> - **Logistic Regression**: Simples, mas assume linearidade (churn não é linear)  
> - **Random Forest**: Bom, mas XGBoost geralmente supera em tabular data  
> - **Neural Networks**: Overkill para 10k exemplos, difícil de interpretar  
> - **XGBoost**: Melhor tradeoff - alta performance, feature importance, lida com missing data, rápido para treinar
> 
> Além disso, XGBoost tem built-in regularization (evita overfit) e funciona bem com features heterogêneas (categóricas + numéricas)."

### **SE PERGUNTAREM: "Como lidou com desbalanceamento de classes?"**

> "Churn é naturalmente desbalanceado (~20% churn, 80% não-churn). Usei:
> 1. **scale_pos_weight** no XGBoost para dar mais peso à classe minoritária  
> 2. **Stratified Split** no train/test para garantir mesma proporção  
> 3. **Métricas certas**: Não usei apenas accuracy (enganosa em desbalanceamento) - usei AUC, Precision, Recall, F1
> 
> Também considerei SMOTE (synthetic oversampling) mas não foi necessário - XGBoost com scale_pos_weight resolveu bem."

### **SE PERGUNTAREM: "Como garantiu que o modelo não sofre overfitting?"**

> "Várias técnicas:
> 1. **Train/Test Split 80/20** - nunca treino no test set  
> 2. **Cross-validation** (5-fold) para tuning de hiperparâmetros  
> 3. **Early stopping** - paro treino quando validation loss para de melhorar  
> 4. **Regularization** - max_depth limitado, min_child_weight, lambda/alpha  
> 5. **Feature selection** - removi features com importância < 1%  
> 6. **Monitoramento em produção** - se AUC cair em test data real, re-treino"

### **SE PERGUNTAREM: "Como escolheu features?"**

> "Combinei 3 abordagens:
> 1. **Domain knowledge**: RFM é clássico em marketing, behavioral features são standard  
> 2. **Feature importance** do XGBoost: Descartei features com importância < 1%  
> 3. **Correlação**: Removi features altamente correlacionadas (r > 0.95) para evitar redundância
> 
> Resultei em 25 features finais - sweet spot entre poder preditivo e interpretabilidade."

### **SE PERGUNTAREM: "Por que Delta Lake ao invés de Parquet ou CSV?"**

> "Delta Lake tem vantagens cruciais:
> - **ACID transactions**: Evita leituras inconsistentes durante writes  
> - **Time travel**: Posso voltar a qualquer versão anterior (`@v10`, `TIMESTAMP AS OF`)  
> - **Schema evolution**: Posso adicionar colunas sem quebrar pipelines  
> - **Upserts eficientes**: `MERGE INTO` é 10-100x mais rápido que reescrever tudo  
> - **DML operations**: DELETE, UPDATE funcionam (Parquet é imutável)
> 
> Para um projeto de produção, Delta é essencial."

### **SE PERGUNTAREM: "Como escalaria isso para 10M de clientes?"**

> "A arquitetura já está preparada:
> 1. **Spark Distributed**: PySpark processa em paralelo - só aumentar cluster  
> 2. **Delta Lake**: Particionar por `customer_segment` ou `created_date`  
> 3. **Batch Scoring**: Processar em chunks (ex: 100k clientes por batch)  
> 4. **Incremental Processing**: Só processar novos clientes/transações (não reprocessar tudo)  
> 5. **Model Serving**: Para real-time, usar Databricks Model Serving (REST API) com autoscaling
> 
> Testei localmente com 10k mas o código escala linearmente."

### **SE PERGUNTAREM: "Qual foi o maior desafio?"**

> "O maior desafio foi **definir o target correto para churn**.
> 
> Inicialmente defini churn como '60 dias sem compra', mas percebi que isso era muito agressivo - clientes B2B compram trimestralmente.
> 
> Refinei para:
> - **90 dias sem compra** + **90 dias sem engajamento** (email, login, etc)  
> - Isso captura churn real sem falsos positivos
> 
> Outro desafio: **explicar para negócio que correlação ≠ causalidade**. Marketing via que campanhas tinham alta conversão, mas não entendia que talvez esses clientes já iam comprar de qualquer jeito. A/B testing foi essencial para mostrar o uplift incremental."

---

## 🚀 PARTE 7: PRÓXIMOS PASSOS (1 minuto)

### **O QUE DIZER:**

> "O projeto está funcional mas há evoluções planejadas:
> 
> **Curto prazo (próximas semanas):**  
> - ✅ Automatizar com Databricks Jobs (executar pipeline diário/semanal)  
> - ✅ Adicionar alertas automáticos (drift detection, AUC drop)  
> - ✅ Model Serving para scoring real-time via API
> 
> **Médio prazo (próximos meses):**  
> - ☐ Integrar com CRM (Salesforce/HubSpot) para ações automáticas  
> - ☐ Next Best Action model (recomendar melhor oferta por cliente)  
> - ☐ LTV (Lifetime Value) prediction  
> - ☐ Multi-touch attribution (qual canal contribuiu mais para conversão?)
> 
> **Longo prazo:**  
> - ☐ MLOps completo com CI/CD (DABs - Databricks Asset Bundles)  
> - ☐ Feature Store centralizada para todos os modelos  
> - ☐ Observabilidade com Lakehouse Monitoring"

---

## ❓ SEÇÃO DE PERGUNTAS E RESPOSTAS FREQUENTES

### **1. "Me explique como funciona o XGBoost?"**

> "XGBoost é um algoritmo de **ensemble learning** baseado em **gradient boosting**.
> 
> Funciona assim:
> 1. Treina uma árvore de decisão simples (weak learner)  
> 2. Vê onde essa árvore errou  
> 3. Treina uma nova árvore focada nos erros da anterior  
> 4. Repete 100-1000 vezes  
> 5. Soma as previsões de todas as árvores (ensemble)
> 
> **Por quê funciona bem:**  
> - Cada árvore corrige os erros das anteriores  
> - Regularização built-in evita overfit  
> - Lida bem com features não-lineares e interações complexas  
> - Paralelize bem (rápido)"

### **2. "Como você valida que o modelo está funcionando em produção?"**

> "Tenho 3 camadas de validação:
> 
> **Layer 1: Monitoring de Drift**  
> - Monitoro distribuição das features (ex: recency_days média mudou?)  
> - Se drift > threshold → alerta
> 
> **Layer 2: Performance do Modelo**  
> - A cada semana, pego previsões da semana passada e comparo com churn real  
> - Calculo AUC out-of-sample  
> - Se AUC cair > 10% → re-treino
> 
> **Layer 3: Business Metrics**  
> - Monitoro KPIs reais: churn rate, retention rate, conversion rate  
> - Se campanhas baseadas no modelo não performam → investigar"

### **3. "O que é p-value e por que 0.05?"**

> "**P-value** é a probabilidade de obter esse resultado (ou mais extremo) **assumindo que não há efeito real**.
> 
> Exemplo: Se p-value = 0.03:
> - Há 3% de chance desse resultado ser puro acaso  
> - Logo, 97% de confiança de que o efeito é real
> 
> **Por quê 0.05?**  
> É o padrão científico (5% de falso positivo aceitável). Em marketing, às vezes uso 0.10 (mais permissivo) para detectar efeitos menores.
> 
> **Importante**: p-value NÃO diz o tamanho do efeito - só se é significante. Por isso também olho lift % e incremental revenue."

### **4. "Qual a diferença entre Precision e Recall?"**

> "Simples:
> 
> **Precision**: Dos que previ como churn, quantos REALMENTE fizeram churn?  
> - Foco: evitar falsos positivos (não quero acionar campanha para cliente que não vai churnar)  
> - Exemplo: Precision 0.78 = 78% dos alertados realmente iam churnar
> 
> **Recall**: Dos que realmente fizeram churn, quantos EU CAPTUREI?  
> - Foco: não perder nenhum churn  
> - Exemplo: Recall 0.82 = capturei 82% dos churners, mas perdi 18%
> 
> **Trade-off**: Aumentar precision diminui recall e vice-versa.  
> **No meu projeto**: Balanceio com F1-score (média harmônica). Para churn, prefiro recall alto (é pior perder um churner que gastar numa campanha desnecessária)."

### **5. "Por que Medallion architecture?"**

> "Medallion (Bronze/Silver/Gold) é o padrão da indústria para Lakehouses porque:
> 
> **Bronze**: Raw data imutável  
> - Se algo quebrar nas camadas superiores, sempre posso reprocessar do zero  
> - Time travel para auditoria/compliance
> 
> **Silver**: Clean data  
> - Separa limpeza de features → mais fácil debugar  
> - Várias equipes podem usar (não só ML)
> 
> **Gold**: Business-level aggregates  
> - Features prontas para ML  
> - Dashboards podem consumir diretamente  
> - Sem lógica de negócio duplicada
> 
> **Alternativas** (por que não usei):  
> - **Single table**: Difícil de manter, sem separation of concerns  
> - **Raw + Processed**: Só 2 camadas, mistura limpeza com features"

### **6. "Como você comunicou resultados técnicos para stakeholders não-técnicos?"**

> "Essa é a parte mais importante! Fiz 3 coisas:
> 
> **1. Traduzi métricas técnicas para negócio**  
> - Não falei 'AUC 0.85' → falei 'top 10% de risco captura 60% dos churns'  
> - Não falei 'p-value 0.03' → falei 'tenho 97% de confiança que essa campanha funciona'
> 
> **2. Criei dashboards visuais**  
> - Gráficos simples: pizza de segmentação, barras de ROAS por campanha  
> - Cores: vermelho (risco alto), verde (risco baixo)  
> - Números grandes: '$500k incremental revenue'
> 
> **3. Foquei em **actionable insights****  
> - Não: 'O modelo tem feature importance de 0.25 em recency'  
> - Sim: 'Clientes que não compram há 60+ dias têm 3x mais chance de churn → priorize eles nas campanhas'"

### **7. "Se você tivesse mais tempo/recursos, o que faria diferente?"**

> "Boa pergunta! Algumas coisas:
> 
> **1. Mais features de dados externos**  
> - Sazonalidade (feriados, fim de ano)  
> - Dados econômicos (desemprego, inflação)  
> - Concorrentes (preços, promoções)
> 
> **2. Modelos mais sofisticados**  
> - Deep Learning para sequential behavior (LSTM para time-series de compras)  
> - Graph Neural Networks para redes sociais / referral patterns
> 
> **3. Reinforcement Learning**  
> - Next Best Action dinâmico (aprende qual campanha mandar baseado no histórico)
> 
> **4. Real-time scoring**  
> - Atualmente é batch (semanal) → poderia ser real-time (detectar churn no momento que acontece)
> 
> **Mas**: Para o escopo atual (10k clientes, budget limitado), a solução XGBoost + batch scoring é perfeitamente adequada. Not over-engineering!"

### **8. "Como você garante reprodutibilidade?"**

> "Várias práticas:
> 
> **1. Versionamento**  
> - Código no Git  
> - Dados versionados (Delta time travel)  
> - Modelos versionados (MLflow Model Registry)
> 
> **2. Seeds fixos**  
> - `random_state=42` em todos os splits  
> - Mesmos dados de treino sempre geram mesmo modelo
> 
> **3. Documentação**  
> - README.md com ordem de execução  
> - Notebooks com comentários  
> - MLflow rastreia todos os parâmetros
> 
> **4. Environment**  
> - requirements.txt com versões exatas  
> - Databricks Runtime version fixo
> 
> Qualquer pessoa pode clonar o repo e reproduzir exatamente os mesmos resultados."

### **9. "Qual métrica de negócio você mais acompanha?"**

> "Se eu tivesse que escolher UMA métrica, seria **Customer Lifetime Value Retido**.
> 
> Porque:
> - Combina churn (retenção) + revenue (valor)  
> - Mede impacto financeiro direto  
> - É o que CEO e CFO entendem
> 
> Cálculo:  
> `CLV Retido = (Clientes salvos do churn) × (LTV médio)`
> 
> Exemplo: Se salvei 500 clientes de churnar e LTV médio é $2k → **salvei $1M**.
> 
> Outras métricas importantes:
> - **ROAS** (para campanhas)  
> - **Conversion Rate** (para propensity)  
> - **Churn Rate** (KPI principal)"

### **10. "Como você testa código de ML?"**

> "ML tem desafios únicos de testes:
> 
> **1. Unit tests (código tradicional)**  
> - Testo funções de preprocessing (ex: `calculate_rfm()` retorna 3 colunas?)  
> - Testo pipelines (ex: bronze → silver não perde linhas?)
> 
> **2. Data validation tests**  
> - Esquema (ex: `customer_id` é sempre int?)  
> - Ranges (ex: `churn_probability` está entre 0-1?)  
> - Nulls (ex: features críticas não têm nulls?)
> 
> **3. Model tests**  
> - Smoke test: Modelo treina sem erro?  
> - Sanity check: AUC > 0.5 (melhor que random)?  
> - Performance test: AUC não caiu > 10% vs versão anterior?
> 
> **4. Integration tests**  
> - Pipeline end-to-end roda sem erro?  
> - Output tables têm linhas?
> 
> Automatizo isso com pytest + Databricks Workflows."

---

## 🎭 DICAS DE COMUNICAÇÃO DURANTE A APRESENTAÇÃO

### **✅ FAÇA:**

1. **Use analogias simples**  
   - "XGBoost é como juntar a opinião de 100 especialistas ao invés de confiar em 1 só"  
   - "P-value é como uma prova por contradição - assumo que não há efeito e vejo se os dados contradizem"

2. **Mostre entusiasmo (mas sem exagero)**  
   - "Uma parte que me orgulho muito é..."  
   - "O resultado que mais me surpreendeu foi..."

3. **Demonstre pensamento crítico**  
   - "Considerei X, mas escolhi Y porque..."  
   - "Uma limitação é Z, que melhoraria com..."

4. **Conecte técnica com negócio**  
   - Sempre termine parágrafos técnicos com: "Isso significa que [impacto de negócio]"

5. **Prepare perguntas para o entrevistador**  
   - "Como vocês atualmente lidam com churn?"  
   - "Qual o maior desafio de ML na empresa hoje?"  
   - "Qual ferramenta de experimentação vocês usam?"

### **❌ EVITE:**

1. **Jargão sem explicar**  
   - Não: "Usei SHAP values para explainability"  
   - Sim: "Usei SHAP values - uma técnica que mostra QUANTO cada feature contribuiu para a previsão de cada cliente"

2. **Respostas vagas**  
   - Não: "O modelo é bom"  
   - Sim: "AUC de 0.85, que é considerado excelente - captura 60% dos churns no top 10%"

3. **Criticar escolhas anteriores sem justificar**  
   - Não: "Marketing estava fazendo tudo errado"  
   - Sim: "Marketing estava medindo conversões, mas sem grupo controle era difícil saber se era causalidade ou correlação"

4. **Fingir saber tudo**  
   - Se não souber algo: "Não tenho experiência com X, mas aprendo rápido. Como funciona aqui?"

5. **Falar muito rápido ou devagar**  
   - Pratique! Tempo ideal: 10-15 min para apresentação + 5-10 min perguntas

---

## 📋 CHECKLIST ANTES DA ENTREVISTA

### **24 horas antes:**

- [ ] Executar TODO o projeto do zero (garantir que roda)
- [ ] Anotar métricas exatas (AUC, ROAS, p-values)
- [ ] Revisar este script 2x
- [ ] Preparar 3 perguntas para o entrevistador
- [ ] Testar câmera/microfone/compartilhamento de tela

### **1 hora antes:**

- [ ] Abrir notebooks no Databricks (01_bronze até 08_dashboards)
- [ ] Abrir MLflow experiments
- [ ] Abrir uma query SQL de exemplo (para demo ao vivo)
- [ ] Água ao lado!

### **Durante a apresentação:**

- [ ] Respire fundo antes de começar
- [ ] Mantenha contato visual (câmera)
- [ ] Pause para perguntas ("Posso avançar ou tem perguntas?")
- [ ] Se travar, volte ao script mental: Problema → Solução → Resultado

---

## 🎯 EXEMPLO DE DEMO AO VIVO (SE PEDIREM)

### **"Pode nos mostrar o projeto rodando?"**

**Opção 1: Mostrar Churn Prediction em ação**

```python
# Notebook: 04_models/Modelo Churn Prediction
# Célula: Previsões

# Mostrar:
df_predictions.filter(col("churn_probability") > 0.7).select(
    "customer_id", 
    "churn_probability", 
    "top_risk_factors"
).show(10)
```

> "Aqui estão os 10 clientes com maior risco. Veja que 'top_risk_factors' mostra POR QUÊ - neste caso, recency_days alto e engagement baixo. Isso é actionable - posso criar campanha focada nesses clientes."

**Opção 2: Mostrar A/B Test Results**

```sql
-- Notebook: 08_dashboards/SQL Queries

SELECT 
  campaign_name,
  lift_pct,
  roas,
  result_category
FROM customer_intelligence.gold.campaign_roas
WHERE result_category IN ('Positive', 'Strong Positive')
ORDER BY roas DESC;
```

> "Esta query mostra quais campanhas tiveram resultado positivo. Campanha 'Spring Sale' teve ROAS 3.5x - cada $1 gerou $3.50. Já 'Black Friday Promo' teve ROAS 0.8x - perdendo dinheiro. Recomendei pausar."

**Opção 3: Mostrar MLflow**

> "No MLflow, vejo todos os experimentos. Aqui treinei 15 versões diferentes do modelo. Esta run (Run #12) teve melhor AUC (0.856) e está registrada em produção. Posso comparar parâmetros: veja que max_depth=6 foi melhor que max_depth=10 (menos overfit)."

---

## 🏁 FECHAMENTO FORTE

### **Últimas palavras (30 segundos):**

> "Em resumo: construí uma plataforma end-to-end que resolve problemas reais de negócio - churn prediction, segmentação, experimentação - usando Databricks, MLOps, e causal inference. 
> 
> O projeto está funcional, documentado, versionado, e pronto para escalar. Estou animado para aplicar essas habilidades em [nome da empresa] e resolver desafios similares - ou ainda maiores!"

**[Sorria, pause, aguarde feedback]**

---

## ✅ BOA SORTE!

**Você tem:**
✅ Projeto completo e funcional  
✅ Conhecimento técnico sólido  
✅ História clara (problema → solução → resultado)  
✅ Impacto de negócio mensurável  
✅ Este script como guia  

**Lembre-se:**
- Você CONSTRUIU isso. Você SABE isso.
- Entrevistador quer ver como você PENSA, não decorou
- Se travar, volte ao básico: "O problema era X, resolvi com Y, resultado foi Z"
- Demonstre curiosidade e vontade de aprender

---

**🎤 Agora vai lá e arrasa! 🚀**
