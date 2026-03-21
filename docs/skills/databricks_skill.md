# Databricks Skill — Execução Incremental no VS Code

## Objetivo

Esta skill define como o agente deve operar quando estiver integrado ao Databricks via VS Code para implementar, executar, validar e corrigir questões de forma incremental, com foco em:
- execução célula a célula;
- avanço por questão;
- diagnóstico preciso de erro;
- correção local;
- uso econômico de recursos computacionais.

Esta skill é operacional.
Ela não substitui policy de escopo de edição nem guideline de implementação PySpark/Spark SQL.

---

## Escopo

Aplicar esta skill quando:
- o arquivo estiver sendo executado no Databricks via VS Code;
- houver necessidade de validar questões por execução real;
- o objetivo for avançar gradualmente, sem rerun massivo;
- a implementação depender de feedback do runtime.

Esta skill é agnóstica à fonte de dados.
O foco é o modo de execução e estabilização incremental.

---

## Responsabilidade da Skill

O agente deve:
- executar no menor escopo viável;
- avançar questão por questão;
- rodar célula a célula sempre que possível;
- evitar consumo massivo de cluster;
- diagnosticar falhas reais do runtime;
- corrigir localmente e reexecutar;
- estabilizar antes de seguir.

O agente não deve:
- reexecutar o arquivo inteiro por padrão;
- disparar execuções amplas sem necessidade;
- usar ações caras repetidamente só para inspeção;
- escalar o escopo de execução sem justificativa técnica.

---

## Princípio Operacional

O agente deve operar com:

**execução incremental + correção iterativa + minimização de custo**

Fluxo padrão:
1. identificar a questão ou célula atual;
2. executar apenas o necessário para validar aquele trecho;
3. observar erro ou resultado;
4. corrigir localmente;
5. reexecutar o menor trecho possível;
6. seguir apenas após estabilização.

---

## Unidade Preferencial de Execução

A ordem de preferência é:

1. célula atual;
2. questão atual;
3. pequena sequência de células com dependência direta;
4. setup mínimo necessário, apenas se o contexto não existir;
5. rerun mais amplo somente se houver perda real de estado.

Regra:
- sempre escolher a menor unidade que permita validação confiável.

---

## Política de Consumo de Recurso

O agente deve assumir que recursos do Databricks são finitos e custosos.

### Obrigatório
- evitar rerun completo do notebook/script;
- evitar `count()` repetido;
- evitar `show()` excessivo;
- evitar `collect()` sem necessidade real;
- evitar `toPandas()` em dados distribuídos;
- evitar `orderBy()` global apenas para inspeção;
- evitar recomputações desnecessárias;
- evitar persist/cache sem reúso claro.

### Preferir
- `show(5)` ou `show(10)` para validação rápida;
- inspeção de schema quando suficiente;
- agregações pequenas para teste;
- validação por amostra quando o enunciado não exigir materialização ampla;
- execução localizada de preparação + consumo imediato.

---

## Execução Célula a Célula

Sempre que possível, o agente deve rodar **célula a célula**.

Objetivos:
- isolar falhas;
- reduzir custo;
- evitar efeitos colaterais;
- facilitar depuração;
- preservar contexto incremental.

### Regras
- não executar células futuras antes da atual estabilizar;
- não rerodar células anteriores sem necessidade;
- não assumir que estado anterior permanece válido sem evidência;
- reexecutar setup apenas quando houver perda de sessão ou dependência ausente.

---

## Uso de PySpark e Spark SQL durante a Execução

Esta skill não decide a modelagem da solução, mas deve preservar clareza operacional entre os modos de execução.

### Quando houver PySpark
Executar a célula ou trecho PySpark diretamente e validar:
- schema;
- colunas esperadas;
- transformações;
- agregações;
- joins;
- windows;
- materialização mínima necessária.

### Quando houver Spark SQL
Antes de executar `spark.sql()`, validar:
- a `temp view` necessária foi criada;
- o nome da view está correto;
- as colunas referenciadas existem;
- a query pode ser executada no contexto atual.

### Quando houver abordagem híbrida
Executar em duas etapas:
1. preparação em PySpark;
2. consulta analítica em Spark SQL.

Manter essa ordem.
Executar SQL sem view pronta é pedir stack trace. Às vezes o cluster responde com sinceridade excessiva.

---

## Diagnóstico de Falhas

Ao ocorrer erro, o agente deve classificar a falha antes de corrigir.

Categorias mínimas:
- sintaxe Python;
- sintaxe SQL;
- nome inexistente;
- coluna inexistente;
- tipo incompatível;
- view não registrada;
- erro de análise Spark;
- erro de execução Spark;
- problema de schema;
- problema de dependência de contexto;
- erro semântico detectado por validação de saída;
- custo excessivo ou execução desnecessariamente ampla.

---

## Processo de Correção Iterativa

Quando uma célula falhar, o agente deve:

1. identificar exatamente onde falhou;
2. identificar a menor causa provável;
3. corrigir apenas o necessário;
4. reexecutar o mesmo trecho;
5. confirmar estabilização antes de avançar.

### Regra
Não transformar falha local em refatoração global.
A tentação existe. A skill não autoriza.

---

## Ordem de Priorização na Correção

Corrigir nesta ordem:
1. falha estrutural de execução;
2. erro de sintaxe;
3. dependência ausente;
4. erro de API;
5. erro de schema/tipo;
6. erro semântico;
7. desperdício computacional relevante;
8. melhoria de clareza.

---

## Critério para Avançar de Questão

O agente só deve avançar quando a questão atual estiver em um destes estados:
- executa sem erro;
- produz saída coerente com o enunciado;
- não introduz dependência inválida para a próxima;
- não exige rerun amplo para continuar.

Status aceitáveis:
- **Estável**
- **Estável com ressalva explícita**

Status não aceitáveis:
- **Parcial sem registro**
- **Quebrada**
- **Não validada**

---

## Sinais de Que o Escopo de Execução Está Grande Demais

O agente deve reduzir o escopo quando notar:
- reruns frequentes do notebook inteiro;
- múltiplas ações amplas para uma única questão;
- repetição de leituras pesadas;
- validações baseadas em `count()` sucessivos;
- dependência desnecessária entre questões independentes;
- uso de operações globais apenas para inspeção.

---

## Exceções para Reexecução Mais Ampla

É aceitável ampliar a execução apenas quando:
- a sessão do cluster foi reiniciada;
- o contexto base foi perdido;
- uma view temporária necessária deixou de existir;
- uma dependência anterior realmente precisa ser refeita;
- há evidência de estado inconsistente.

Mesmo nesses casos:
- reexecutar apenas o mínimo para restaurar o contexto.

---

## Checkpoints Operacionais

Antes de cada execução, validar:
- qual célula será executada;
- qual dependência mínima ela possui;
- se há risco de custo desnecessário;
- se existe validação mais barata;
- se o contexto atual é suficiente.

Antes de cada avanço, validar:
- a célula estabilizou;
- a saída faz sentido;
- não há erro pendente mascarado;
- o contexto para a próxima etapa continua íntegro.

---

## Antiapadrões Proibidos

- rerun completo por hábito;
- `count()` em série para “ver se está funcionando”;
- `collect()` para depuração de lógica distribuída;
- `toPandas()` sem necessidade explícita;
- execução de várias questões ao mesmo tempo sem motivo;
- corrigir sem ler a stack trace;
- manter célula cara rodando repetidamente para inspeção cosmética;
- ampliar escopo de execução por conveniência.

---

## Critério de Aplicação Correta da Skill

A skill foi aplicada corretamente quando:
- a execução ocorreu de forma incremental;
- o agente avançou por célula ou questão;
- o consumo de recurso foi contido;
- erros foram corrigidos localmente;
- o runtime foi usado como mecanismo real de validação;
- não houve rerun massivo sem necessidade.

---

## Regra Final

Princípio obrigatório:

**executar no menor escopo viável, validar cedo, corrigir localmente e evitar custo desnecessário.**