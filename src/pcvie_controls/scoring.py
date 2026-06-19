from pcvie_controls.algorithms import (
    calculate_customer_score,
    calculate_email_quality,
    get_algorithm_version,
)
from pcvie_controls.validation import validate_customer


def control_customer(customer: dict) -> dict:
    """
    Point d'entrée métier appelé par l'orchestration Databricks.
    """

    customer_id = customer.get("customer_id")
    first_name = customer.get("first_name")
    last_name = customer.get("last_name")
    email = customer.get("email")

    errors = validate_customer(customer_id, first_name, last_name, email)

    if errors:
        status = "KO"
    else:
        status = "OK"

    return {
        "customer_id": customer_id,
        "full_name": f"{first_name} {last_name}",
        "email": email,
        "customer_score": calculate_customer_score(customer_id, email),
        "email_quality": calculate_email_quality(email),
        "control_status": status,
        "control_errors": ",".join(errors),
        "algorithm_version": get_algorithm_version(),
    }