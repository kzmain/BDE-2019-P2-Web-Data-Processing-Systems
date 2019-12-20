###     ==============================================================================
###     |                                                                            |
###     |   This class is envisioned as an abstract NLP class                        |
###     |       So that any NLP libary implemented can be easily swapped             |
###     |       for the currently used one (Spacy) as all code is dependent          |
###     |       on the NLP class having this function (generate_entities(payload))   |
###     |                                                                            |
###     ==============================================================================

class NLP:
    @staticmethod
    def generate_entities(payload):
        return NotImplementedError
