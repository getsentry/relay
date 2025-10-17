"""
Fix for GitHub Enterprise Integration Config Silo Boundary Violation

This file demonstrates the fix for the silo boundary violation that occurs when
REGION silo services try to directly access CONTROL silo Integration models.
"""

from typing import Optional, Dict, Any
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class SiloMode(Enum):
    """Sentry silo modes"""
    MONOLITH = "monolith"
    CONTROL = "control" 
    REGION = "region"


class ObjectStatus:
    """Object status constants"""
    ACTIVE = 1
    DISABLED = 0


class IntegrationProviderSlug:
    """Integration provider constants"""
    GITHUB_ENTERPRISE = "github_enterprise"


# Mock classes to demonstrate the pattern
class Integration:
    """Mock Integration model"""
    def __init__(self, id, provider, external_id, metadata):
        self.id = id
        self.provider = provider
        self.external_id = external_id
        self.metadata = metadata


class SiloAwareIntegrationService:
    """
    Fixed integration service that properly handles silo boundaries
    """
    
    @staticmethod
    def get_current_silo_mode() -> SiloMode:
        """Get the current silo mode - this would be implemented by Sentry framework"""
        # This is a mock implementation
        # In real Sentry code, this would check the actual silo configuration
        return SiloMode.REGION
    
    def get_integration(
        self, 
        integration_id: int,
        provider: str,
        organization_id: int,
        status: int = ObjectStatus.ACTIVE
    ) -> Optional[Integration]:
        """
        Get integration with silo-aware access
        
        This method automatically routes to the appropriate data access method
        based on the current silo mode.
        """
        current_mode = self.get_current_silo_mode()
        
        if current_mode == SiloMode.REGION:
            # In REGION mode, use hybrid cloud service for cross-silo communication
            return self._get_integration_via_hybrid_cloud(
                integration_id, provider, organization_id, status
            )
        else:
            # In CONTROL or MONOLITH mode, direct model access is safe
            return self._get_integration_direct(
                integration_id, provider, organization_id, status
            )
    
    def _get_integration_via_hybrid_cloud(
        self,
        integration_id: int,
        provider: str, 
        organization_id: int,
        status: int
    ) -> Optional[Integration]:
        """
        Get integration via hybrid cloud service (for REGION silo)
        """
        try:
            # This would use Sentry's hybrid cloud integration service
            # to make a cross-silo RPC call to the CONTROL silo
            from sentry.services.hybrid_cloud.integration import integration_service
            
            integration_data = integration_service.get_integration(
                integration_id=integration_id,
                provider=provider,
                organization_id=organization_id,
                status=status
            )
            
            if not integration_data:
                return None
                
            # Convert hybrid cloud response to Integration object
            return Integration(
                id=integration_data.id,
                provider=integration_data.provider,
                external_id=integration_data.external_id,
                metadata=integration_data.metadata
            )
            
        except Exception as e:
            logger.exception(
                "Failed to get integration via hybrid cloud service",
                extra={
                    "integration_id": integration_id,
                    "provider": provider,
                    "organization_id": organization_id
                }
            )
            return None
    
    def _get_integration_direct(
        self,
        integration_id: int,
        provider: str,
        organization_id: int, 
        status: int
    ) -> Optional[Integration]:
        """
        Get integration via direct model access (for CONTROL/MONOLITH silo)
        """
        try:
            # This would use Django ORM to query the Integration model directly
            # This is safe in CONTROL or MONOLITH mode
            integration_kwargs = {
                'id': integration_id,
                'provider': provider,
                'organizations__id': organization_id,
                'status': status
            }
            
            # In real implementation, this would be:
            # return Integration.objects.get(**integration_kwargs)
            
            # Mock implementation for demonstration
            return Integration(
                id=integration_id,
                provider=provider,
                external_id="mock_external_id",
                metadata={"domain_name": "github.example.com"}
            )
            
        except Exception as e:
            logger.exception(
                "Failed to get integration via direct access",
                extra={
                    "integration_id": integration_id,
                    "provider": provider,
                    "organization_id": organization_id
                }
            )
            return None


class FixedSeerRpcEndpoint:
    """
    Fixed Seer RPC endpoint that uses silo-aware integration service
    """
    
    def __init__(self):
        self.integration_service = SiloAwareIntegrationService()
    
    def get_github_enterprise_integration_config(
        self, 
        organization_id: int, 
        integration_id: str
    ) -> Dict[str, Any]:
        """
        Get GitHub Enterprise integration configuration
        
        This method now uses the silo-aware integration service to avoid
        silo boundary violations.
        """
        try:
            # Convert integration_id to int
            integration_id_int = int(integration_id)
            
            # Use silo-aware service to get integration
            integration = self.integration_service.get_integration(
                integration_id=integration_id_int,
                provider=IntegrationProviderSlug.GITHUB_ENTERPRISE,
                organization_id=organization_id,
                status=ObjectStatus.ACTIVE
            )
            
            if not integration:
                logger.warning(
                    "GitHub Enterprise integration not found",
                    extra={
                        "integration_id": integration_id,
                        "organization_id": organization_id
                    }
                )
                return {
                    "success": False,
                    "error": "Integration not found"
                }
            
            # Extract configuration from integration metadata
            domain_name = integration.metadata.get("domain_name", "")
            
            config = {
                "success": True,
                "integration_id": integration.id,
                "provider": integration.provider,
                "external_id": integration.external_id,
                "base_url": f"https://{domain_name}" if domain_name else "",
                "api_url": f"https://{domain_name}/api/v3" if domain_name else "",
                "installation_id": integration.external_id,
                "domain_name": domain_name,
                "metadata": integration.metadata
            }
            
            logger.info(
                "Successfully retrieved GitHub Enterprise integration config",
                extra={
                    "integration_id": integration_id,
                    "organization_id": organization_id,
                    "domain_name": domain_name
                }
            )
            
            return config
            
        except ValueError as e:
            logger.error(
                "Invalid integration_id format",
                extra={
                    "integration_id": integration_id,
                    "organization_id": organization_id,
                    "error": str(e)
                }
            )
            return {
                "success": False,
                "error": f"Invalid integration_id format: {integration_id}"
            }
            
        except Exception as e:
            logger.exception(
                "Failed to get GitHub Enterprise integration config",
                extra={
                    "integration_id": integration_id,
                    "organization_id": organization_id
                }
            )
            return {
                "success": False,
                "error": "Internal server error"
            }


# Example usage and testing
def test_silo_aware_integration_access():
    """
    Test the silo-aware integration access
    """
    endpoint = FixedSeerRpcEndpoint()
    
    # Test successful config retrieval
    result = endpoint.get_github_enterprise_integration_config(
        organization_id=1118521,
        integration_id="261546"
    )
    
    print("Integration config result:", result)
    
    # Test error handling
    result = endpoint.get_github_enterprise_integration_config(
        organization_id=1118521,
        integration_id="invalid_id"
    )
    
    print("Error handling result:", result)


if __name__ == "__main__":
    test_silo_aware_integration_access()