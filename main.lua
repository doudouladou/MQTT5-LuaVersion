local mqtt5 = require "mqtt5"
local host = "imadaydreamer.cn"
local port = 1883
local client_id = "12345678"
local username
local password
local keepalive
local clean_session
local object
local publish_topic = "abcdefghijklmnopqrstuvwxyz"
local will_topic = "will"
local function mqtt_client_event_cbfunc(mqtt_client, event, data, payload, metas)
    log.info("mqtt_client_event_cbfunc", mqtt_client, event, data, payload)
    if event == "connack" then
        sys.publish("connack")
    end
end

local will = {payload = "this client is dead", topic = will_topic, retain = 1, qos = 0, property = {delay_interval = 200}}

local property = {topic_alias_max_len = 60000}

-- local will = nil
sys.taskInit(function()
    sys.waitUntil("IP_READY")
    -- 创建一个mqtt5 client
    object = mqtt5.create(client_id, username, password, keepalive, clean_session, will, property)

    -- 注册用户回调
    object:on(mqtt_client_event_cbfunc)

    -- 连接服务器
    object:connect(host, 1883)

    -- 等待连接成功
    sys.waitUntil("connack")

    -- 订阅主题
    object:subscribe("SubTest", 1)
    local pubproperty = {
        alias = 2000
    }
    -- 往主题发布数据
    -- object:publish(publish_topic, "" .. os.time(), 1, 1, pubproperty)
    -- sys.wait(1000)
    -- while 1 do
    --     log.info("aaaaa")
    --     -- 往主题发布数据
    --     object:publish("", "" .. os.time(), 1, 1, pubproperty)
    --         -- mqtt5.publish(object, "PubTest", "" .. os.time(), 0, 1, pubproperty)
    --     sys.wait(1000)
    -- end
end)

sys.run()
