from pydantic import BaseModel
from prefect.blocks.system import Secret

class FLA_Prefect(BaseModel):

    def create_secret_block(self, name: str, value: str) -> None:

        secret_block = Secret(value = value)
        secret_block.save(name = name, overwrite = True)

        print(f"Saved to Secret -> {name} 🔒")

        return None 