<!--
╭───────────────────────────────╮
│      PULL REQUEST TEMPLATE    │
│      Projeto Databricks       │
╰───────────────────────────────╯
-->

# Resumo

> Explique, em 1-2 frases, **o que** este PR faz e **por quê**.

---

## Tipo de mudança

- [ ] 🐛 Correção de bug
- [ ] ✨ Nova feature / notebook
- [ ] 📚 Documentação
- [ ] ⚙️ Infraestrutura (CI/CD, Jobs, Clusters, Pipelines)
- [ ] ♻️ Refatoração (nenhuma mudança funcional)
- [ ] 🔒 Segurança / Permissões
- [ ] Outro: <!-- descreva -->

---

## Alterações principais

| Categoria               | Descrição rápida |
|-------------------------|------------------|
| **Notebooks**          | `/<path>/<file>.ipynb` <!-- liste notebooks criados/alterados --> |
| **SQL / Delta Lake**   | Tabelas afetadas, esquemas adicionados/alterados, migrações |
| **Jobs / Pipelines**   | Jobs atualizados/criados, cron, DLT pipelines, triggers |
| **Config. de Cluster** | Modo runtime, libs instaladas, autoscaling, init scripts |
| **Permissões**         | Grupos, Table ACLs, cluster policies |
| **Dependências**       | Bibliotecas PyPI/Maven novas ou atualizadas |
| **Outros**             | <!-- dashboards, MLflow, etc. --> |

---

## Checklist

- [ ] Código segue os **padrões de estilo** definidos (lint/black/isort, mypy, etc.).
- [ ] **Testes unitários** relevantes adicionados/atualizados.
- [ ] **Testes de integração**/pipeline executados em ambiente de staging.
- [ ] **Documentação** (README, comentários nos notebooks, docs internas) atualizada.
- [ ] **Permissões** revisadas (Unity Catalog, Table ACLs, cluster policies).
- [ ] Nenhum dado sensível ou credencial foi incluído no código/notebook.
- [ ] Se altera dado em produção, existe **plano de rollback**.

---

## Como testar

1. **Provisionar ambiente**: instruções para criar cluster/pipeline de teste (runtime, configs).
2. **Executar notebooks**: passos ou script para rodar notebooks em sequência.
3. **Validar resultados**: queries SQL, checksums, dashboards, métricas, etc.
4. **Logs/monitoramento**: onde verificar (Jobs UI, MLflow, observability).

---

## Evidências

- **Capturas de tela** ou GIFs (visualizações, dashboards, notebooks executados).
- Links para **runs de job**/página de pipeline de teste.
- Output de comandos `pytest` ou `dbx test`.

---

## Issues relacionadas

Closes #123, Relates to #456

---

## Aprovação

> Marque revisores obrigatórios, tech leads ou owners de tabela/serviço.

- [ ] Revisor 1:
- [ ] Revisor 2:

---

<!-- Obrigado por contribuir! -->
