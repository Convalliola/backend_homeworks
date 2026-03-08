class AppError(Exception):
    """Базовая ошибка приложения (не HTTP)"""


class ModelNotLoadedError(AppError):
    """Модель не загружена/недоступна"""


class PredictionError(AppError):
    """Ошибка во время предсказания"""


class AdvertisementNotFoundError(AppError):
    """Объявление не найдено"""