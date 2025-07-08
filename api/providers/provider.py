from api.models.meetings import MeetingItem


class Provider:

    @staticmethod
    def create(
        provider_name: str, authority: str, config: dict | None = None
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

        return SpecificProvider(authority=authority, config=config)

    def __init__(self, authority: str, config: dict | None = None):
        self.authority = authority
        self.config = config

        self.index: list | None = None

    def build_index(self) -> list[MeetingItem]:
        """
        Build the index for the provider.
        This method should be implemented by subclasses.
        The index exists to enable filtering, but expensive calls shouldn't be made here.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def get_meetings(self, index: list[MeetingItem]) -> list[MeetingItem]:
        """
        Get meetings for the provider.
        This method should be implemented by subclasses.
        This method should add values to the index, and can make expensive calls.
        """
        raise NotImplementedError("Subclasses must implement this method.")


from api.providers.publici import PublicI
