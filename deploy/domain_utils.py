import smtplib
import socket
import logging
from urllib.parse import urlparse
import dns.resolver

logger = logging.getLogger(__name__)

def generate_email_patterns(domain: str) -> list[str]:
    """
    Extracts the root domain and generates a list of candidate emails.
    """
    if not domain:
        return []
    
    # Strip protocols and paths if present
    if "://" in domain:
        domain = urlparse(domain).netloc
    
    # Simple extraction of root domain for the prefix
    parts = domain.split('.')
    if len(parts) >= 2:
        root = parts[-2]
    else:
        root = parts[0]
        
    patterns = [
        f"founder@{domain}",
        f"ceo@{domain}",
        f"team@{domain}",
        f"sales@{domain}",
        f"contact@{domain}",
        f"hello@{domain}",
        f"support@{domain}",
        f"info@{domain}",
        f"{root}@{domain}"
    ]
    return patterns

def validate_email_domain(domain: str) -> bool:
    """
    Performs a DNS MX record lookup to verify the domain supports email.
    """
    if not domain:
        return False
        
    if "://" in domain:
        domain = urlparse(domain).netloc
        
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        return len(answers) > 0
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, dns.exception.Timeout):
        return False
    except Exception as e:
        logger.warning(f"Error checking MX records for {domain}: {e}")
        return False

def get_mx_servers(domain: str) -> list[str]:
    """
    Returns the MX servers for a domain, sorted by preference.
    """
    if "://" in domain:
        domain = urlparse(domain).netloc
        
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        # Sort by preference
        mx_records = sorted(answers, key=lambda a: a.preference)
        return [str(r.exchange).rstrip('.') for r in mx_records]
    except Exception:
        return []

def verify_email_smtp(email: str, mx_servers: list[str] = None) -> bool:
    """
    Performs a lightweight SMTP connection and handshake (HELO, MAIL FROM, RCPT TO)
    to verify if the mailbox exists without actually sending an email.
    Uses confidence 0.8 if true, 0.5 if false but domain valid.
    """
    if not email or '@' not in email:
        return False
        
    domain = email.split('@')[1]
    
    if mx_servers is None:
        mx_servers = get_mx_servers(domain)
        
    if not mx_servers:
        return False
        
    for mx in mx_servers:
        try:
            # Connect to SMTP server
            server = smtplib.SMTP(timeout=3)
            server.set_debuglevel(0)
            server.connect(mx)
            
            # Initiate handshake
            server.helo(server.local_hostname)
            
            # Send MAIL FROM
            server.mail(f"ping@{domain}") # Dummy sender
            
            # Send RCPT TO
            code, message = server.rcpt(email)
            
            server.quit()
            
            if code == 250:
                logger.debug(f"SMTP verified email {email} on {mx}")
                return True
            else:
                logger.debug(f"SMTP verification failed for {email} on {mx} with code {code}")
                return False
                
        except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, socket.error) as e:
            logger.debug(f"SMTP connection error to {mx} for {email}: {e}")
            continue # Try next MX
        except Exception as e:
            logger.debug(f"Unexpected SMTP error for {email}: {e}")
            continue
            
    return False
