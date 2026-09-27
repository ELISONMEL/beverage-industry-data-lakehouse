# Data Quality Rules

## Clientes
- `cliente_id` obrigatório
- `nome_cliente` obrigatório
- `estado` deve conter 2 letras
- `status_cliente` deve ser `ATIVO` ou `INATIVO`
- `tipo_cliente` deve pertencer ao domínio permitido
- `data_cadastro` deve ser válida

## Pedidos
- identificadores obrigatórios
- data válida
- valores não negativos
- desconto não pode ser maior que o valor bruto
- `valor_total = valor_bruto - valor_desconto`

## Itens
- quantidade positiva
- preço positivo
- desconto entre 0 e 100
- total do item deve reconciliar com quantidade, preço e desconto
