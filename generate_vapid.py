"""Generate VAPID keys for Web Push notifications and save to file"""
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import base64

# Generate EC key pair
key = ec.generate_private_key(ec.SECP256R1(), default_backend())

# Get private key in PEM format
priv_pem = key.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption()
).decode()

# Get public key in uncompressed point format, then base64url encode
pub_raw = key.public_key().public_bytes(
    serialization.Encoding.X962,
    serialization.PublicFormat.UncompressedPoint
)
pub_b64 = base64.urlsafe_b64encode(pub_raw).decode().rstrip('=')

# Write to file
with open('vapid_keys_generated.env', 'w', encoding='utf-8') as f:
    f.write("# VAPID Keys for Web Push Notifications\n")
    f.write("# Add these lines to your .env file\n\n")
    f.write(f"VAPID_PUBLIC_KEY={pub_b64}\n\n")
    # Escape the private key for env file
    escaped_priv = priv_pem.replace('\n', '\\n')
    f.write(f"VAPID_PRIVATE_KEY={escaped_priv}\n\n")
    f.write("VAPID_CLAIMS_EMAIL=mailto:admin@almudeer.com\n")

print("Keys generated and saved to: vapid_keys_generated.env")
print(f"Public key: {pub_b64[:40]}...")
