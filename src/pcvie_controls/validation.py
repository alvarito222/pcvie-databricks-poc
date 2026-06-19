def validate_customer(customer_id: int, first_name: str, last_name: str, email: str) -> list[str]:
    """
    Retourne une liste d'erreurs fonctionnelles.
    """
    errors = []

    if customer_id is None:
        errors.append("MISSING_CUSTOMER_ID")

    if not first_name:
        errors.append("MISSING_FIRST_NAME")

    if not last_name:
        errors.append("MISSING_LAST_NAME")

    if not email:
        errors.append("MISSING_EMAIL")
    elif "@" not in email:
        errors.append("INVALID_EMAIL_FORMAT")

    return errors