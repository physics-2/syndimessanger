package ru;

import it.tdlight.Init;
import it.tdlight.Log;
import it.tdlight.Slf4JLogMessageHandler;
import it.tdlight.client.*;
import it.tdlight.jni.TdApi;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class Example {

    // 1. ПУБЛИЧНЫЕ КЛЮЧИ (от официального Telegram Desktop).
    // Они легальны и работают для личных аккаунтов.
    private static final int API_ID = 21724;
    private static final String API_HASH = "5255282f86f82df8152347764f492df1";

    public static void main(String[] args) throws Exception {
        // Инициализация нативных библиотек
        Init.init();
        Log.setLogMessageHandler(9999, new Slf4JLogMessageHandler());

        try (SimpleTelegramClientFactory clientFactory = new SimpleTelegramClientFactory()) {

            // 2. ИСПОЛЬЗУЕМ РЕАЛЬНЫЕ КЛЮЧИ, а не example()
            APIToken apiToken = new APIToken(API_ID, API_HASH);

            TDLibSettings settings = TDLibSettings.create(apiToken);

            // 3. ОТКЛЮЧАЕМ тестовый дата-центр! Нам нужен реальный Telegram.
            // settings.setUseTestDatacenter(true); <-- ЭТУ СТРОКУ МЫ УДАЛЯЕМ/КОММЕНТИРУЕМ

            Path sessionPath = Paths.get("tdlib-session-real");
            settings.setDatabaseDirectoryPath(sessionPath.resolve("data"));
            settings.setDownloadedFilesDirectoryPath(sessionPath.resolve("downloads"));

            SimpleTelegramClientBuilder clientBuilder = clientFactory.builder(settings);

            // 4. ИСПОЛЬЗУЕМ КОНСОЛЬНЫЙ ВВОД для реального номера телефона
            // 4. ИСПОЛЬЗУЕМ КОНСОЛЬНЫЙ ВВОД, чтобы программа могла спросить и номер, и код
            ConsoleInteractiveAuthenticationData authenticationData = AuthenticationSupplier.consoleLogin();

            try (var app = new ExampleApp(clientBuilder, authenticationData)) {
                System.out.println("✅ Приложение запущено и ожидает сообщений. Нажмите Ctrl+C для выхода.");

                // Держим главный поток живым
                Thread.currentThread().join();
            }
        }
    }

    public static class ExampleApp implements AutoCloseable {
        private  SimpleTelegramClient client;
        private boolean proxyAdded = false; // Флаг, чтобы добавить прокси только один раз

        public ExampleApp(SimpleTelegramClientBuilder clientBuilder,
                          ConsoleInteractiveAuthenticationData authenticationData) {

            // 5. БЕЗОПАСНОЕ ДОБАВЛЕНИЕ ПРОКСИ
            // Мы добавляем его ровно в тот момент, когда TDLib спрашивает номер телефона.
            // Это гарантирует, что прокси применится ДО любой попытки соединения с сервером.
            clientBuilder.addUpdateHandler(TdApi.UpdateAuthorizationState.class, update -> {
                TdApi.AuthorizationState state = update.authorizationState;

                if (state instanceof TdApi.AuthorizationStateWaitPhoneNumber && !proxyAdded) {
                    proxyAdded = true;
                    System.out.println("\n🔧 [СИСТЕМА] Обнаружен запрос номера. Внедряю локальный прокси...");

                    // ВНИМАНИЕ: Замените 1080 на порт вашего локального прокси (v2rayN, Clash и т.д.)
                    // Обычно это 1080 (SOCKS5) или 7890 (HTTP/SOCKS5)

                    TdApi.Proxy proxy = new TdApi.Proxy("127.0.0.1",10808,new TdApi.ProxyTypeSocks5("incy_bdb8ac16","1c016d2cf39e415f31270d6bf1e2f352"));
                    TdApi.AddProxy request;

                    request = new TdApi.AddProxy(proxy,true,"");


                    client.send(request, result -> {
                        if (result.isError()) {
                            System.err.println("❌ Ошибка добавления прокси: " + result.getError());
                            System.err.println("💡 Убедитесь, что ваша VPN-программа запущена и порт (1080) верный!");
                        } else {
                            System.out.println("✅ [СИСТЕМА] Прокси успешно добавлен! Теперь вводите номер телефона.");
                        }
                    });
                } else if (state instanceof TdApi.AuthorizationStateReady) {
                    System.out.println("\n🎉 [СИСТЕМА] Успешная авторизация! Аккаунт подключен.");
                }
            });

            // Обработчик новых сообщений (из вашего примера)
            clientBuilder.addUpdateHandler(TdApi.UpdateNewMessage.class, this::onUpdateNewMessage);

            // Сборка и запуск клиента
            this.client = clientBuilder.build(authenticationData);
        }

        @Override
        public void close() throws Exception {
            client.close();
        }

        public SimpleTelegramClient getClient() {
            return client;
        }

        private void onUpdateNewMessage(TdApi.UpdateNewMessage update) {
            TdApi.MessageContent messageContent = update.message.content;
            String text;

            if (messageContent instanceof TdApi.MessageText messageText) {
                text = messageText.text.text;
            } else {
                text = String.format("(%s)", messageContent.getClass().getSimpleName());
            }

            long chatId = update.message.chatId;

            client.send(new TdApi.GetChat(chatId))
                    .whenCompleteAsync((chat, error) -> {
                        if (error != null) {
                            System.out.printf("💬 Новое сообщение в чате %s: %s%n", chatId, text);
                        } else {
                            System.out.printf("💬 [%s]: %s%n", chat.title, text);
                        }
                    });
        }
    }
}