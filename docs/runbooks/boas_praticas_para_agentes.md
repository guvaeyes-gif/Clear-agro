# Boas Praticas para Agentes

## Regras de atuacao
- Trabalhar por escopo claro de modulo.
- Nao sobrescrever trabalho funcional de outro dominio.
- Ler antes de alterar, especialmente em `automation`, `database`, `security` e `logs`.
- Publicar documentacao quando a mudanca alterar ownership, fluxo ou risco.

## Regras de seguranca
- Nao mover credenciais para o repositorio.
- Nao alterar scheduler sem evidenciar impacto e rollback.
- Nao executar duas mudancas criticas ao mesmo tempo no mesmo modulo.

## Regras de compatibilidade
- Preservar caminhos produtivos ate a convergencia ser validada.
- Tratar artefato legado como risco documentado, nao como lixo descartavel.
- Preferir wrappers e transicao gradual a renomeacao agressiva.

## Regras de observabilidade
- Toda automacao nova deve publicar status.
- Toda falha relevante deve ser rastreavel por log e status.
- Toda mudanca critica deve deixar trilha em inventario ou auditoria.
