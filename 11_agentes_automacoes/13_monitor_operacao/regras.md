# 13_monitor_operacao - Regras

## Regras obrigatorias
- Nunca executar pagamentos, aprovacoes ou alteracoes criticas sem aprovador humano.
- Registrar log de toda acao relevante em logs/.
- Indicar fonte de dados usada em cada resposta.
- Escalar bloqueios para 00_orquestrador_clear em ate 15 min.
- Seguir principio de menor privilegio de acesso.
- Nao expor dados sensiveis fora do canal autorizado.

## Regras de qualidade
- Informar periodo analisado (data/hora inicial e final).
- Sinalizar grau de confianca da analise (alto/medio/baixo).
- Indicar proximo passo com dono e prazo.
