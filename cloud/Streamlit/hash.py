import hashlib

# Contraseña original
contrasena_original = "holamundo"

# Calcular el hash SHA-256 de la contraseña
hash_contraseña = hashlib.sha256(contrasena_original.encode()).hexdigest()

# Imprimir el hash
print(f"CONTRASENA_HASH={hash_contraseña}")