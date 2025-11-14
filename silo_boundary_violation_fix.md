# GitHub Enterprise Integration Config Silo Boundary Violation Fix

## Issue Summary
**Error**: `InitializationError: Error getting github enterprise integration config`
**Root Cause**: REGION silo service directly accessing CONTROL-plane Integration model, causing `SiloLimit.AvailabilityError` and `400 Bad Request`.

## Technical Analysis

### Error Flow
1. Seer service → Sentry RPC endpoint (`get_github_enterprise_integration_config`)
2. `@region_silo_endpoint` processes request in REGION mode
3. `DatabaseBackedIntegrationService.get_integration()` attempts direct model access
4. `Integration.objects.get()` fails due to silo boundary violation
5. Results in HTTP 400 response to Seer

### Silo Architecture Context
- **CONTROL silo**: Contains organization, integration, and user management data
- **REGION silo**: Contains event processing, issues, and project-specific data
- **Boundary violation**: REGION silo code cannot directly access CONTROL silo models

## Solution Implementation

### 1. Fix Integration Service Implementation

The `DatabaseBackedIntegrationService.get_integration()` method needs to be made silo-aware:

```python
# Current problematic implementation
def get_integration(self, integration_id, provider, organization_id, status):
    # This fails in REGION silo mode
    integration = Integration.objects.get(**integration_kwargs)
    return integration

# Fixed silo-aware implementation  
def get_integration(self, integration_id, provider, organization_id, status):
    from sentry.silo.base import SiloMode
    from sentry.services.hybrid_cloud.integration import integration_service
    
    if SiloMode.get_current_mode() == SiloMode.REGION:
        # Use hybrid cloud service for cross-silo communication
        return integration_service.get_integration(
            integration_id=integration_id,
            provider=provider,
            organization_id=organization_id,
            status=status
        )
    else:
        # Direct model access is safe in CONTROL or MONOLITH mode
        integration_kwargs = {
            'id': integration_id,
            'provider': provider,
            'organizations__id': organization_id,
            'status': status
        }
        return Integration.objects.get(**integration_kwargs)
```

### 2. Update Seer RPC Endpoint

The `get_github_enterprise_integration_config` method should use the hybrid cloud integration service:

```python
def get_github_enterprise_integration_config(self, organization_id: int, integration_id: str):
    from sentry.services.hybrid_cloud.integration import integration_service
    from sentry.integrations.models import IntegrationProviderSlug
    
    try:
        # Use hybrid cloud service instead of direct service call
        integration = integration_service.get_integration(
            integration_id=int(integration_id),
            provider=IntegrationProviderSlug.GITHUB_ENTERPRISE.value,
            organization_id=organization_id,
            status=ObjectStatus.ACTIVE,
        )
        
        if not integration:
            return {"success": False, "error": "Integration not found"}
            
        # Extract configuration from integration
        config = {
            "success": True,
            "base_url": integration.metadata.get("domain_name", ""),
            "api_url": f"https://{integration.metadata.get('domain_name', '')}/api/v3",
            "installation_id": integration.external_id,
            # Add other required configuration fields
        }
        
        return config
        
    except Exception as e:
        logger.exception("Failed to get GitHub Enterprise integration config")
        return {"success": False, "error": str(e)}
```

### 3. Alternative: Move Endpoint to Control Silo

If the above approach doesn't work due to service limitations, consider moving the endpoint to the CONTROL silo:

```python
# Change from:
@region_silo_endpoint
class SeerRpcServiceEndpoint(Endpoint):
    # ...

# To:
@control_silo_endpoint  # or @all_silo_endpoint if needed
class SeerRpcServiceEndpoint(Endpoint):
    # ...
```

### 4. Hybrid Cloud Service Enhancement

Ensure the hybrid cloud integration service supports all required operations:

```python
# In integration service interface
class IntegrationService:
    def get_integration(
        self, 
        integration_id: int, 
        provider: str, 
        organization_id: int, 
        status: int
    ) -> Integration | None:
        """Get integration with silo-aware access"""
        pass
        
    def get_github_enterprise_config(
        self,
        integration_id: int,
        organization_id: int
    ) -> dict:
        """Get GitHub Enterprise specific configuration"""
        pass
```

## Implementation Steps

1. **Update Integration Service**: Make `get_integration()` method silo-aware
2. **Update RPC Endpoint**: Use hybrid cloud service instead of direct service calls
3. **Test Cross-Silo Communication**: Verify the fix works in both REGION and CONTROL modes
4. **Add Error Handling**: Ensure graceful degradation if hybrid cloud service fails
5. **Update Documentation**: Document the silo-aware integration access patterns

## Testing Strategy

1. **Unit Tests**: Test silo-aware integration service methods
2. **Integration Tests**: Test Seer → Sentry RPC communication
3. **Silo Mode Tests**: Verify behavior in different silo modes
4. **Error Handling Tests**: Test failure scenarios and error responses

## Migration Considerations

- This fix maintains backward compatibility
- No database schema changes required
- Existing integration data remains accessible
- Performance impact should be minimal due to caching in hybrid cloud services

## Success Criteria

- ✅ No more `SiloLimit.AvailabilityError` exceptions
- ✅ GitHub Enterprise integration config retrieval works in REGION silo
- ✅ Seer autofix functionality resumes normal operation
- ✅ No regression in other integration-related features