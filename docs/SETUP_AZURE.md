# Configuração Azure AD para Power BI

Este guia detalha como configurar a autenticação do Azure AD para acessar a API do Power BI.

## Pré-requisitos

- Conta Azure com acesso ao Azure Active Directory
- Licença Power BI Pro ou Premium
- Permissões de administrador no tenant Azure

## Opção 1: Service Principal (Recomendado para Produção)

### 1. Registrar Aplicação no Azure AD

1. Acesse [Azure Portal](https://portal.azure.com)
2. Navegue para **Azure Active Directory** → **App registrations**
3. Clique em **New registration**
4. Configure:
   - **Name**: `PowerBI-Chat-Integration`
   - **Supported account types**: Single tenant
   - **Redirect URI**: Deixe em branco por enquanto

### 2. Criar Client Secret

1. Na aplicação criada, vá para **Certificates & secrets**
2. Clique em **New client secret**
3. Configure:
   - **Description**: `Production Key`
   - **Expires**: 24 months (ajuste conforme necessidade)
4. **COPIE O VALUE** imediatamente (não será exibido novamente)

### 3. Configurar Permissões de API

1. Vá para **API permissions**
2. Clique em **Add a permission**
3. Selecione **Power BI Service**
4. Adicione as seguintes permissões **Delegated**:
   - `Dataset.Read.All`
   - `Dataset.ReadWrite.All`
   - `Workspace.Read.All`
   - `Report.Read.All`
   - `Dashboard.Read.All`

5. Para Service Principal, adicione também **Application** permissions:
   - `Tenant.Read.All`

6. Clique em **Grant admin consent** (requer admin)

### 4. Habilitar Service Principal no Power BI

1. Acesse [Power BI Admin Portal](https://app.powerbi.com/admin-portal)
2. Vá para **Tenant settings**
3. Na seção **Developer settings**:
   - Habilite **Allow service principals to use Power BI APIs**
   - Adicione o grupo de segurança da aplicação

### 5. Adicionar Service Principal ao Workspace

1. No Power BI, abra o workspace desejado
2. Vá para **Access**
3. Adicione a aplicação com role **Member** ou **Admin**

### 6. Configurar Variáveis de Ambiente

```bash
AZURE_AD_TENANT_ID=<seu-tenant-id>
AZURE_AD_CLIENT_ID=<seu-client-id>
AZURE_AD_CLIENT_SECRET=<seu-client-secret>
POWERBI_WORKSPACE_ID=<workspace-id>
POWERBI_AUTH_MODE=service-principal
```

## Opção 2: User Token (Para Desenvolvimento)

### 1. Configurar MSAL

A aplicação usa MSAL para autenticação interativa do usuário.

```typescript
// Configuração MSAL
const msalConfig = {
  auth: {
    clientId: process.env.AZURE_AD_CLIENT_ID,
    authority: `https://login.microsoftonline.com/${process.env.AZURE_AD_TENANT_ID}`,
    redirectUri: 'http://localhost:3000/auth/callback',
  },
};
```

### 2. Adicionar Redirect URI

1. No Azure Portal, vá para a aplicação registrada
2. **Authentication** → **Add a platform**
3. Selecione **Single-page application**
4. Configure:
   - **Redirect URIs**: `http://localhost:3000/auth/callback`
   - Habilite **Access tokens** e **ID tokens**

## Opção 3: XMLA Endpoint (Premium)

Para acesso direto ao modelo semântico via XMLA:

### Pré-requisitos
- Power BI Premium ou Premium Per User (PPU)
- XMLA endpoint habilitado no workspace

### Configuração

1. No Power BI Admin Portal, habilite **XMLA Endpoints** para leitura
2. Configure a connection string:

```bash
POWERBI_XMLA_ENDPOINT=powerbi://api.powerbi.com/v1.0/myorg/WorkspaceName
```

3. Use bibliotecas como `adomd-client` ou `pyadomd` para conexão

## Troubleshooting

### Erro: "AADSTS7000215: Invalid client secret"
- Verifique se o secret não expirou
- Confirme que copiou o **Value** e não o **Secret ID**

### Erro: "Unauthorized (401)"
- Confirme que o Service Principal foi adicionado ao workspace
- Verifique se as permissões de API foram concedidas
- Certifique-se de que o admin consent foi dado

### Erro: "Forbidden (403) - Insufficient permissions"
- O Service Principal precisa de role Admin ou Member no workspace
- Verifique as permissões de API no Azure AD

### Erro: "Rate limit exceeded"
- A API tem limites de requisições
- Implemente exponential backoff
- Considere caching de resultados

## Limites da API

| Recurso | Limite |
|---------|--------|
| Chamadas por hora | 200 por usuário |
| Chamadas por minuto | 120 por tenant |
| Tamanho máximo de resultado | 100.000 linhas |
| Timeout de query | 5 minutos |

## Segurança

### Recomendações

1. **Nunca commite secrets** - Use variáveis de ambiente ou Key Vault
2. **Rotacione secrets** regularmente
3. **Use menor privilégio** - Apenas permissões necessárias
4. **Monitore uso** - Habilite logging de acesso à API
5. **Separe ambientes** - Service Principals diferentes para dev/prod

### Key Vault (Recomendado para Produção)

```bash
# Armazenar secret no Key Vault
az keyvault secret set \
  --vault-name "my-keyvault" \
  --name "powerbi-client-secret" \
  --value "seu-secret-aqui"

# Referenciar no código
const secret = await secretClient.getSecret("powerbi-client-secret");
```
