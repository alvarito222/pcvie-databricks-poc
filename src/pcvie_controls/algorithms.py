from datetime import datetime, timezone


def calculate_customer_score(customer_id: int, email: str) -> int:
    """
    Algorithme simplifié de scoring.
    Dans le vrai cas, ce fichier représenterait un algorithme actuariel.
    """
    score = 0

    if customer_id is not None:
        score += customer_id * 10

    if email and email.endswith("@example.com"):
        score += 25

    return score


def calculate_email_quality(email: str) -> str:
    """
    Exemple de règle de contrôle simple.
    """
    if email is None or "@" not in email:
        return "INVALID"

    if email.endswith("@example.com"):
        return "STANDARD_DOMAIN"

    return "OTHER_DOMAIN"


def get_algorithm_version() -> str:
    """
    Permet de tracer quelle version logique de l'algorithme a été utilisée.
    """
    return "customer-control-v1.0.0"