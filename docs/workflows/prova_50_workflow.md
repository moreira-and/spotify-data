## Processo executado (com justificativa técnica)

---

### 1. Geração inicial da solução

**Entradas:**

* `politica.md`
* `especialista_pyspark.md`
* `prova_50.py`

**Ação:**
Execução de uma task para resolver a prova conforme os anexos.

**Por que essa etapa existe:**

* Produzir uma **baseline funcional inicial**
* Traduzir requisitos da prova em implementação concreta
* Aplicar restrições da política desde o início (evitar retrabalho estrutural)

**Observação crítica:**
Essa saída não é confiável por definição — é apenas um ponto de partida.

---

### 2. Revisão estruturada da solução

**Entradas:**

* `politica.md`
* `revisor_pyspark.md`
* `prova_50.py`

**Ação:**
Execução de uma task de revisão, gerando um plano (`PROVA_50_plano.md`).

**Por que essa etapa existe:**

* Separar **execução de avaliação** (reduz viés do agente)
* Identificar:

  * erros lógicos
  * inconsistências
  * problemas de performance
* Produzir um **plano acionável**, em vez de correções diretas

**Decisão importante:**
O revisor não altera código → força rastreabilidade e controle.

---

### 3. Refatoração guiada por plano

**Entradas:**

* `politica.md`
* `especialista_pyspark.md`
* `PROVA_50_plano.md`
* `prova_50.py`

**Ação:**
Execução de uma task de refatoração baseada no plano gerado.

**Por que essa etapa existe:**

* Evitar refatoração arbitrária
* Garantir que mudanças:

  * sejam justificadas
  * tenham origem rastreável
* Transformar o agente em **executor de decisões**, não em tomador de decisões

**Ganho técnico:**
Redução de comportamento não determinístico (alucinação estrutural).

---

### 4. Debug com integração ao Databricks

**Entradas:**

* `politica.md`
* `especialista_pyspark.md`
* `revisor_pyspark.md`
* `PROVA_50_refatoracao.md`
* `prova_50.py`
* erros retornados pela execução na plataforma

**Ação:**
Execução de uma task de debug orientada pelos erros reais da plataforma.

**Por que essa etapa existe:**

* Introduzir **feedback de execução real (runtime)**
* Corrigir:

  * erros de sintaxe não detectados
  * incompatibilidades com Spark/Databricks
  * falhas de execução distribuída

**Ponto chave:**
Sem runtime, o processo valida apenas plausibilidade — não execução.

---

### 5. Debug e validação humana

**Ação:**

* Ajustes manuais para viabilizar execução
* Revisão final do atendimento das questões

**Por que essa etapa existe:**

* Validar aderência ao enunciado (LLM não garante isso)
* Garantir:

  * completude das respostas
  * coerência semântica
  * ausência de regressões

**Limitação estrutural do agente:**
LLMs não possuem entendimento real de “correto” — apenas de “provável”.

---

## Síntese do racional do processo

Cada etapa resolve uma limitação específica:

| Etapa           | Problema que resolve              |
| --------------- | --------------------------------- |
| Geração inicial | Ausência de solução               |
| Revisão         | Viés e erros ocultos              |
| Plano           | Falta de direcionamento           |
| Refatoração     | Mudanças caóticas                 |
| Debug runtime   | Falta de validação real           |
| Humano          | Falta de entendimento de contexto |

---

## Leitura correta do processo

Não é um fluxo de automação simples.

É um pipeline com três camadas de validação:

1. **Sintática/estrutural** → especialista
2. **Lógica/arquitetural** → revisor
3. **Execução real** → Databricks
4. **Semântica/contextual** → humano

Se você remove qualquer uma, a qualidade degrada rapidamente.

---

Se quiser evoluir essa documentação:
→ próximo passo seria formalizar isso como um **workflow padrão reutilizável (template YAML ou orquestrador)**.
