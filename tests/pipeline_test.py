import unittest
from pipeline import Pipeline
from cores.step import Step

# Étapes de test simples qui simulent le comportement ETL
class MockExtract(Step):
    """Simule l'extraction de données depuis une source"""
    def execute(self, data=None):
        # Retourne des données brutes simulées
        return [1, 2, 3]

class MockTransform(Step):
    """Simule la transformation des données"""
    def execute(self, data):
        # Double chaque valeur des données
        return [x * 2 for x in data]

class MockLoad(Step):
    """Simule le chargement des données"""
    def execute(self, data):
        # Sauvegarde les données pour vérification
        self.saved_data = data
        return data

class TestPipeline(unittest.TestCase):
    """Test l'intégration complète du pipeline ETL"""

    def test_pipeline_run(self):
        # Création des étapes mockées
        extract = MockExtract()    # Étape d'extraction
        transform = MockTransform() # Étape de transformation
        load = MockLoad()          # Étape de chargement

        # Construction du pipeline avec les 3 étapes
        pipeline = Pipeline(steps=[extract, transform, load])

        # Exécution du pipeline complet
        result = pipeline.run()

        # Vérifications :
        self.assertEqual(extract.execute(), [1, 2, 3])  # ✅ Teste Extract
        self.assertEqual(result, [2, 4, 6])  # ✅ Teste le résultat final
        self.assertEqual(load.saved_data, [2, 4, 6])  # ✅ Teste Loades

if __name__ == '__main__':
    # Lance les tests unitaires
    unittest.main()