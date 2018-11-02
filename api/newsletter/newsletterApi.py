import os

def save_subscriber( email_adress):
    """Check if email adress already exists in subscribers file
    If yes, return error.
    If no, add email to file and return success"""
    file_path = "./user-data/newsletter_subscribers.txt"
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            for line in file:
                if line == ("%s\n" % email_adress):
                    return {
                        'error': 'email already exists'
                    }

    with open(file_path, "a") as file:
        file.write("%s\n" % email_adress)
        return {
            'email_subscription': 'success'
        }
