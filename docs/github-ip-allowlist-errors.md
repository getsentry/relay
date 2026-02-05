# Handling GitHub IP Allowlist Errors in Upstream Services

## Issue Context (SENTRY-5H77)

This document describes how to handle GitHub API errors caused by IP allowlist restrictions in upstream services that integrate with GitHub.

## Problem Description

When upstream services (such as Seer or other GitHub-integrated services) attempt to access GitHub repositories, they may encounter 403 Forbidden errors if:

1. The GitHub organization has IP allowlist restrictions enabled
2. The service's deployment IP address is not whitelisted
3. The error is not properly handled, resulting in a 500 Internal Server Error

## Error Flow

Example flow from an external service (this is illustrative; actual implementation may vary):

```
1. Client → Application → API endpoint
2. Application → Service → API call (e.g., /v1/automation/autofix/state)  
3. Service → Repository access check function
4. Service → GitHub API (check repository access)
5. GitHub → 403 Forbidden (IP not whitelisted)
6. Unhandled GithubException bubbles up
7. Service → 500 Internal Server Error
8. Application ← HTTPError
```

## Recommended Solution

### For Services Integrating with GitHub:

1. **Catch GitHub API Exceptions Specifically**:
   ```python
   try:
       github_client.get_repo(repo_name)
   except GithubException as e:
       if e.status == 403:
           # Check if this is an IP allowlist issue
           if "ip allow list" in e.data.get('message', '').lower():
               raise GitHubIPAllowlistError(
                   f"GitHub organization has IP allowlist enabled. "
                   f"Please whitelist this service's IP address or disable IP restrictions."
               ) from e
       raise
   ```

2. **Return Proper Error Responses**:
   Instead of letting exceptions bubble up as 500 errors, return a structured error:
   ```python
   return {
       "error": "github_access_denied",
       "message": "Cannot access GitHub repository due to IP allowlist restrictions",
       "action": "contact_github_admin",
       "details": {
           "organization": org_name,
           "repository": repo_name,
           "reason": "ip_allowlist"
       }
   }, 403
   ```

3. **Log Detailed Information**:
   ```python
   logger.warning(
       "GitHub API access denied due to IP allowlist",
       extra={
           "github_org": org_name,
           "github_repo": repo_name,
           "status_code": 403,
           "error_type": "ip_allowlist",
           "suggested_action": "whitelist_ip"
       }
   )
   ```

### For Relay (This Service):

While Relay does not directly interact with GitHub, it forwards errors from upstream services. The current error handling in `relay-server/src/services/upstream.rs` already properly handles:

- 403 Forbidden responses with RelayErrorAction::Stop (permanent rejection)
- Network errors (502-504)
- Rate limiting (429)
- Generic response errors

If GitHub-related errors from upstream services need special handling, they should include proper error codes and messages in the response body.

## Testing

To test IP allowlist error handling:

1. Configure a GitHub organization with IP allowlist enabled
2. Attempt to access the repository from an IP not on the allowlist
3. Verify that:
   - The error is caught and logged with appropriate detail
   - A user-friendly error message is returned (not 500)
   - The error includes actionable information (e.g., "Contact your GitHub admin")

## Related Issues

- SENTRY-5H77: Seer service fails to check GitHub repo access due to IP allowlist restrictions

## Additional Resources

- [GitHub IP Allowlist Documentation](https://docs.github.com/en/organizations/keeping-your-organization-secure/managing-allowed-ip-addresses-for-your-organization)
- GitHub API Error Codes: https://docs.github.com/en/rest/overview/resources-in-the-rest-api#client-errors
