from typing import List
from cores.step import Step

class Pipeline:
    def __init__(self, steps: List[Step]):
        self.steps = steps

    def run(self):
        data = None
        for i, step in enumerate(self.steps):
            if i == 0:  # première étape
                data = step.execute()
            else:
                data = step.execute(data)
        return data