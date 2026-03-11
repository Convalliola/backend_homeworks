class AppError(Exception):
    """Базовая ошибка приложения (не HTTP)"""


class ModelNotLoadedError(AppError):
    """Модель не загружена/недоступна"""


class PredictionError(AppError):
    """Ошибка во время предсказания"""


class AdvertisementNotFoundError(AppError):
    """Объявление не найдено"""


class AuthenticationError(AppError):
    """Неверный логин или пароль"""


class AccountBlockedError(AppError):
    """Аккаунт заблокирован"""


class InvalidTokenError(AppError):
    """Невалидный или просроченный токен"""