local mqtt5 = require "mqtt5"
local host = "imadaydreamer.cn"
local port = 1883
local client_id = "12345678"
local username
local password
local keepalive
local cleansession
local object

local function mqtt_client_event_cbfunc(mqtt_client, event, data, payload, metas)
    log.info("mqtt_client_event_cbfunc", mqtt_client, event, data, payload)
    if event == "connack" then
        sys.publish("connack")
    end
end

sys.taskInit(function()
    sys.waitUntil("IP_READY")
    -- 创建一个mqtt5对象，实际上返回的是一个table
    object = mqtt5.create(client_id, username, password, keepalive, cleansession, nil, {topic_alias_max_len = 60000})

    -- 注册用户回调
    mqtt5.on(object, mqtt_client_event_cbfunc)

    -- 连接服务器
    mqtt5.connect(object, host, 1883)

    -- 等待连接成功
    sys.waitUntil("connack")

    -- 订阅主题
    mqtt5.subscribe(object, "SubTest", 0)
    local pubproperty = {
        alias = 2000
    }
    -- 往主题发布数据
    mqtt5.publish(object, "PubTest", "" .. os.time(), 0, 1, pubproperty)
    sys.wait(1000)
    while 1 do
        -- 往主题发布数据
        mqtt5.publish(object, "", "" .. os.time(), 0, 1, pubproperty)
            -- mqtt5.publish(object, "PubTest", "" .. os.time(), 0, 1, pubproperty)
        sys.wait(1000)
    end
end)

sys.run()
