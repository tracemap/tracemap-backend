import os

def save_subscriber( email_adress):
    file_path = "./user-data/newsletter_subscribers.txt"
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            for line in file:
                if line == ("%s\n" % email_adress):
                    return {
                        'error': 'email already exists'
                    }
    else:
        open(file_path,"a").close()

    with open(file_path, "a") as file:
        file.write("%s\n" % email_adress)
        return {
            'email_subscription': 'success'
        }
