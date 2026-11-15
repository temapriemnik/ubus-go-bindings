# ubus-go-bindings

Go-биндинг к OpenWrt `ubus` через официальные C-заголовки
`libubus`, `libubox` и `libblobmsg-json`.

Библиотека не реализует wire-протокол ubus заново: все операции выполняются
через `libubus`/`uloop`. C-файл в `internal/native/bridge.c` — это только
тонкий cgo-слой (trampolines и владение C-памятью) между Go и уже
существующими заголовками/функциями libubus. Полностью убрать C-файл нельзя:
cgo не позволяет передавать Go-функции напрямую как C-callbacks; здесь нет
собственной реализации ubus, только обвязка над официальным API.

## Требования

- Linux;
- cgo;
- Go 1.23+;
- заголовки и библиотеки:
  - `libubus`,
  - `libubox`,
  - `libblobmsg-json`,
  - `libjson-c`;
- запущенный `ubusd`;
- тулчейн, соответствующий target (OpenWrt musl, glibc и т.д.).

Пакет использует system headers. Для OpenWrt cross-build нужны именно
target-заголовки из `staging_dir`, а не заголовки хоста:

```sh
CGO_ENABLED=1 GOOS=linux GOARCH=arm64 \
CC="$STAGING_DIR/toolchain-*/bin/aarch64-openwrt-linux-musl-gcc" \
CGO_CFLAGS="-I$STAGING_DIR/target-*/usr/include" \
CGO_LDFLAGS="-L$STAGING_DIR/target-*/usr/lib" \
go build ./...
```

или через Makefile:

```sh
make cross \
  CROSS_GOARCH=arm64 \
  CROSS_CC="$STAGING_DIR/toolchain-*/bin/aarch64-openwrt-linux-musl-gcc" \
  CROSS_CFLAGS="-I$STAGING_DIR/target-*/usr/include" \
  CROSS_LDFLAGS="-L$STAGING_DIR/target-*/usr/lib"
```

## Проверенные версии

Тестирование выполнялось в Docker-образе
`golang:1.27.3-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0`.

| Компонент | Версия |
|---|---|
| OpenWrt `ubus` | commit `60e04048a0e2f3e33651c19e62861b41be4c290f` (2025-10-17) |
| OpenWrt `libubox` | commit `49056d178f42da98048a5d4c23f83a6f6bc6dd80` (2025-07-23) |
| `libubus` ABI | `2025-01-02` |
| `libubox` ABI | `2024-03-29` |
| GCC | `gcc (Debian 12.2.0-14+deb12u1) 12.2.0` |
| Go | `go1.27.3` |
| Debian | `12.15` (bookworm) |
| libjson-c | `0.16-2` |
| Архитектура тестов | `linux/amd64` |

Эти версии закреплены в `docker/testenv/Dockerfile` и передаются в
`make testenv` из `Makefile` (аргументы `UBUS_COMMIT`, `LIBUBOX_COMMIT`).
Для другой версии OpenWrt поменяйте commit-ы и пересоберите образ.

## Сборка и тесты

Все команды — через `Makefile`.

| Команда | Что делает |
|---|---|
| `make testenv` | собирает Docker-образ с закреплёнными ubus/libubox |
| `make test` | **полный** тест: unit + integration + `-race` в Docker |
| `make test-integration` | то же, что `make test` |
| `make test-nocgo` | проверяет сборку без cgo (`CGO_ENABLED=0`) на хосте |
| `make build` | собирает пакет в Docker с native-зависимостями |
| `make cross CROSS_CC=... CROSS_CFLAGS=... CROSS_LDFLAGS=...` | cross-build под OpenWrt target |
| `make vet` | `go vet` в Docker |
| `make fmt` | форматирование `gofumpt` |
| `make fmt-check` | проверка форматирования |
| `make check` | `fmt-check` + `test-nocgo` + `test` + `vet` |
| `make clean` | удаляет локальный тестовый образ |

Пример:

```sh
make check
```

Docker-тест сам собирает C-fixture, запускает `ubusd` на временном сокете,
поднимает тестовый ubus-объект и проверяет вызовы, события, подписки,
уведомления, fd-passing, monitor и reconnect. Контейнер тестов запускается
от root: `ubusd` применяет ACL к клиентам с ненулевым uid, а тестовый
`ubusd` работает без ACL-файла. Сам образ по умолчанию использует
непривилегированного пользователя `ubus`.

## Быстрый старт

### Вызов метода

```go
package main

import (
	"context"
	"fmt"
	"time"

	ubus "github.com/artem/ubus-go-bindings"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := ubus.Dial(ctx, ubus.Options{
		Socket:         "/var/run/ubus/ubus.sock",
		DefaultTimeout: 3 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var response struct {
		Uptime int64 `json:"uptime"`
	}
	if err := conn.Invoke(ctx, "system", "info", map[string]any{}, &response); err != nil {
		panic(err)
	}
	fmt.Println(response.Uptime)
}
```

### Публикация объекта

```go
type EchoRequest struct {
	Value string `json:"value"`
}

type EchoResponse struct {
	Value string `json:"value"`
}

publication, err := conn.Publish(ctx, ubus.Object{
	Name: "example.echo",
	Methods: []ubus.Method{
		ubus.NewMethod("echo", ubus.MethodOptions{
			Policy: ubus.Policy{
				{Name: "value", Type: ubus.String, Required: true},
			},
		}, func(_ context.Context, req EchoRequest) (EchoResponse, error) {
			return EchoResponse{Value: req.Value}, nil
		}),
	},
})
if err != nil {
	panic(err)
}
defer publication.Close(context.Background())
```

### События

```go
listener, err := conn.Listen(ctx, "myapp.*", func(ctx context.Context, event ubus.Event) {
	fmt.Printf("event=%s body=%s\n", event.Name, event.Body)
})
if err != nil {
	panic(err)
}
defer listener.Close(context.Background())

if err := conn.SendEvent(ctx, "myapp.changed", map[string]any{"value": 1}); err != nil {
	panic(err)
}
```

### Подписка и уведомления

```go
subscription, err := conn.Subscribe(ctx, "example.echo",
	func(ctx context.Context, notification ubus.Notification) (ubus.Reply, error) {
		return ubus.Reply{Body: []byte(`{"ack":true}`)}, nil
	})
if err != nil {
	panic(err)
}
defer subscription.Close(context.Background())

report, err := publication.Notify(ctx, "changed", map[string]any{"value": 2})
if err != nil {
	panic(err)
}
fmt.Println(report.Results, report.Replies)
```

## Публичный API

### Соединение

| Метод | Назначение |
|---|---|
| `Dial(ctx, Options) (*Conn, error)` | подключиться к `ubusd`, запустить uloop-поток |
| `(*Conn).Close() error` | закрыть соединение, завершить deferred-запросы |
| `(*Conn).Done() <-chan struct{}` | закрывается после `Close` |
| `(*Conn).Err() error` | терминальная ошибка соединения |

### Объекты и вызовы

| Метод | Назначение |
|---|---|
| `(*Conn).Lookup(ctx, path) ([]ObjectInfo, error)` | найти объекты по path |
| `(*Conn).LookupID(ctx, path) (uint32, error)` | получить native ID объекта |
| `(*Conn).Invoke(ctx, object, method, req, resp) error` | вызов с JSON-кодированием |
| `(*Conn).InvokeRaw(ctx, object, method, req) (json.RawMessage, error)` | вызов с raw JSON |
| `(*Conn).InvokeWithFD(ctx, object, method, req, requestFD, resp) (*os.File, error)` | вызов с передачей fd |
| `(*Conn).Publish(ctx, Object) (*Publication, error)` | опубликовать объект и методы |
| `(*Publication).Close(ctx) error` | снять объект |
| `(*Publication).HasSubscribers() bool` | есть ли подписчики |
| `(*Publication).SubscriberChanges() <-chan bool` | переходы 0 ↔ N подписчиков |
| `(*Publication).Notify(ctx, type, body) (NotifyReport, error)` | уведомить подписчиков и дождаться статусов |
| `(*Publication).NotifyNow(type, body) error` | no-reply уведомление |

### События

| Метод | Назначение |
|---|---|
| `(*Conn).Listen(ctx, pattern, handler) (*EventListener, error)` | подписка на события по pattern |
| `(*Conn).SendEvent(ctx, name, body) error` | отправить событие |
| `(*EventListener).Close(ctx) error` | снять обработчик |
| `(*EventListener).Dropped() uint64` | сколько событий отброшено из-за очереди |

### Подписки

| Метод | Назначение |
|---|---|
| `(*Conn).Subscribe(ctx, object, handler) (*Subscription, error)` | подписка на объект |
| `(*Subscription).Changes() <-chan SubscriptionState` | состояния target |
| `(*Subscription).Close(ctx) error` | снять подписку |

### Monitor

| Метод | Назначение |
|---|---|
| `(*Conn).Monitor(ctx, MonitorOptions) (*Monitor, error)` | включить raw monitor |
| `(*Monitor).Records() <-chan MonitorRecord` | поток записей |
| `(*Monitor).Dropped() uint64` | сколько записей отброшено |
| `(*Monitor).Close(ctx) error` | выключить monitor |

### Вспомогательные типы

- `Options`, `Object`, `Method`, `MethodOptions`, `Policy`, `Field`,
  `FieldType`, `MethodTags`;
- `Request`, `Reply`, `ACLKey`;
- `ObjectInfo`, `Event`, `Notification`, `SubscriptionState`;
- `Status`, `StatusError`, sentinel-ошибки (`ErrNotFound`,
  `ErrTimeout`, `ErrDisconnected`, ...);
- `NewMethod`, `NewRawMethod`.

## Какие C-функции используются

C-слой вызывает только публичный API libubus/libubox:

| Группа | Функции |
|---|---|
| Соединение | `ubus_connect`, `ubus_free`, `ubus_reconnect`, `ubus_add_uloop` |
| Event loop | `uloop_init`, `uloop_run`, `uloop_end`, `uloop_done`, `uloop_fd_add/delete`, `uloop_timeout_*` |
| Поиск | `ubus_lookup`, `ubus_lookup_id` |
| Вызовы | `ubus_invoke_async_fd`, `ubus_complete_request_async`, `ubus_abort_request` |
| Объекты | `ubus_add_object`, `ubus_remove_object`, `ubus_send_reply`, `ubus_defer_request`, `ubus_complete_deferred_request` |
| fd | `ubus_request_get_caller_fd`, `ubus_request_set_fd` |
| События | `ubus_register_event_handler`, `ubus_unregister_event_handler`, `ubus_send_event` |
| Подписки | `ubus_register_subscriber`, `ubus_subscribe`, `ubus_unsubscribe`, `ubus_unregister_subscriber` |
| Уведомления | `ubus_notify`, `ubus_notify_async` |
| Monitor | `ubus_monitor_start`, `ubus_monitor_stop` |
| blobmsg | `blobmsg_add_json_from_string`, `blobmsg_format_json`, `blobmsg_parse`, `blobmsg_add_field`, `blob_parse`, `blob_parse_untrusted` |

## Модель потоков и владения

- `uloop` глобален, поэтому в v1 допускается **один `Conn` на процесс**.
- Все вызовы libubus выполняются на отдельном OS-thread с
  `runtime.LockOSThread`.
- Публичные методы `Conn` можно вызывать из нескольких goroutine.
- Callbacks из C в Go только копируют данные и складывают их в bounded-очередь;
  пользовательские handler-ы выполняются вне uloop-потока.
- Паника в handler ловится и превращается в `UBUS_STATUS_UNKNOWN_ERROR`;
  deferred-запрос всегда завершается.
- C-память принадлежит bridge. Go получает только числовые ID и копии строк.
- Входящий fd: `Request.TakeFD()` передаёт владение вызывающему; если не
  вызван, библиотека закроет fd после handler-а.
- Исходящий fd в `Reply.FD` передаёт владение библиотеке; она закрывает его
  после `dup` или после неудачного завершения.
- `InvokeWithFD` не забирает владение `requestFD`; возвращённый `*os.File`
  принадлежит вызывающему.

## Reconnect

`Options.Reconnect = true` включает восстановление после потери `ubusd`:

- незавершённые операции завершаются `ErrDisconnected` и не повторяются;
- объекты перерегистрируются средствами libubus;
- event handlers, подписки и активный monitor восстанавливаются библиотекой;
- подписки повторно резолвятся по имени target с retry;
- удаление объекта/подписки во время разрыва обрабатывается без освобождения
  памяти, на которую ещё ссылается libubus.

## Ограничения и безопасность

- v1: один `Conn` на процесс из-за глобального uloop.
- Raw monitor — привилегированная диагностика. В записях могут быть
  credentials и конфигурация; не логируйте их без необходимости.
- Ошибки и логи библиотеки не содержат payload-ов по умолчанию.
- HTTP JSON-RPC, rpcd, UCI, ubus CLI и wire-протокол не реализуются.
- Для OpenWrt используйте target-тулчейн и target-заголовки; ABI
  `struct ubus_*` должен совпадать с библиотекой на устройстве.

## Лицензия

MIT. См. `LICENSE`.
