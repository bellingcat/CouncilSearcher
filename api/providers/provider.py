from api.providers.publici import PublicI
from api.models.meetings import MeetingItem


class Provider:

    @staticmethod
    def create(
        provider_name: str, db_path: str, authority: str, config: dict | None = None
    ) -> "Provider":
        """
        Factory method to create a provider instance based on the provider name.
        """
        provider_classes = {
            "publici": PublicI,
        }

        if provider_name.lower() not in provider_classes:
            raise ValueError(f"Provider '{provider_name}' is not supported.")

        SpecificProvider = provider_classes[provider_name.lower()]

        return SpecificProvider(db_path=db_path, authority=authority, config=config)

    def __init__(self, db_path: str, authority: str, config: dict | None = None):
        self.db_path = db_path
        self.authority = authority
        self.config = config

        self.index: list | None = None

    def build_index(self) -> None:
        """
        Build the index for the provider.
        This method should be implemented by subclasses and should populate the `self.index` attribute.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def get_meetings(self) -> list[MeetingItem]:
        """
        Get meetings for the provider.
        This method should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")
