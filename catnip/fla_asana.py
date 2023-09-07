from pydantic import BaseModel, SecretStr
import asana

class FLA_Asana(BaseModel):

    api_token: SecretStr
    project_id: SecretStr
    
    def create_task(
        self,
        task_due_date: str,
        task_html_body: str,
        task_name: str
    ) -> None:

        parameters = {
            "due_on": task_due_date,
            "html_notes": task_html_body,
            "name": task_name,
            "projects": self.project_id.get_secret_value()
        }

        client = asana.Client.access_token(self.api_token.get_secret_value())
        result = client.tasks.create_task(parameters, opt_pretty = True)
        print(result)

        return None 