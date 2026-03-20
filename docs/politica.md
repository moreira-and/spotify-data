# Policy — Restrição de Implementação em Blocos Autorizados

## Objetivo

Estabelecer a regra obrigatória de que qualquer implementação em scripts, provas, notebooks ou pipelines seja realizada exclusivamente nos blocos explicitamente destinados à solução, preservando integralmente a estrutura original do arquivo.

---

## Diretriz Mandatória

O agente deve substituir somente ocorrências de:

```python
# SEU CÓDIGO AQUI
````

A substituição deve ocorrer apenas no bloco local correspondente, sem alterar qualquer outro trecho do arquivo.

---

## Escopo

Esta policy se aplica a:

* scripts
* notebooks exportados
* provas práticas
* exercícios técnicos
* pipelines
* templates com blocos reservados para implementação

A regra é agnóstica ao domínio, à tecnologia de origem dos dados e ao tipo de exercício.

---

## Regras Obrigatórias

O agente deve:

* implementar apenas dentro do bloco autorizado;
* preservar setup, imports, enunciados, markdown, ordenação e cleanup;
* manter o contexto integral de cada questão;
* garantir que a alteração não quebre a execução do script ou pipeline;
* respeitar as restrições específicas do enunciado local.

O agente não deve:

* editar fora do bloco `# SEU CÓDIGO AQUI`;
* remover questões, células, comentários ou instruções;
* reestruturar o arquivo;
* mover lógica para outro ponto do script;
* alterar comportamento global sem autorização explícita;
* introduzir mudanças que comprometam etapas subsequentes.

---

## Comentário Técnico do Agente

Quando necessário, o agente pode registrar uma observação técnica dentro do próprio bloco substituído, no formato:

```python
#AGENT [Nome do agente]: Comentário
```

Esse comentário:

* deve permanecer no lugar do bloco original;
* deve ser objetivo e tecnicamente justificável;
* não substitui a implementação quando a implementação for possível.

---

## Tratamento de Ambiguidade

Se a instrução não fornecer contexto suficiente para garantir conformidade estrutural ou técnica, o agente deve solicitar esclarecimento antes de editar.

Na ausência de ambiguidade material, a implementação deve prosseguir.

---

## Critério de Conformidade

A execução estará em conformidade com esta policy somente se:

* a alteração ocorrer exclusivamente no bloco autorizado;
* o restante do arquivo permanecer inalterado;
* nenhuma questão ou etapa for removida;
* o fluxo do script/pipeline permanecer íntegro;
* eventuais comentários do agente seguirem o padrão definido.

---

## Regra Final

Princípio operacional obrigatório:

**preservar o arquivo, substituir apenas `# SEU CÓDIGO AQUI`, manter o contexto e não quebrar a execução.**