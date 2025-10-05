local mqtt5 = {}
mqtt5.__index = mqtt5

local ConnectFixHead = 0x10
local ConnackFixHead = 0x20
local PublishFixHead = 0x30
local PubackFixHead = 0x40
local PubrecFixHead = 0x50
local PubrelFixHead = 0x60
local PubcompFixHead = 0x70
local SubscribeFixHead = 0x80
local SubackFixHead = 0x90
local UnsubscribeFixHead = 0xA0
local UnsubackFixHead = 0xB0
local PintReqFixHead = 0xC0
local PingRespFixHead = 0xD0
local DisconnectFixHead = 0xE0
local AuthFixHead = 0xF0

local function encode_len(len)
    local s = ""
    local digit
    repeat
        digit = len % 128
        len = (len - digit) / 128
        if len > 0 then
            digit = bit.bor(digit, 0x80)
        end
        s = s .. string.char(digit)
    until (len <= 0)
    return s
end

local function ping_req()
    local str = string.char(PintReqFixHead, 0x00)
    return str
end

local function pack_puback(id)
    local str = string.char(PubackFixHead, 0x04) .. id .. string.char(0x00, 0x00)
    return str
end

local function pack_pubrec(id)
    local str = string.char(PubrecFixHead, 0x04) .. id .. string.char(0x00, 0x00)
    return str
end

local function pack_pubrel(id)
    local str = string.char(PubrelFixHead, 0x04) .. id .. string.char(0x00, 0x00)
    return str
end

local MqttPublicAnalysis = {
    [ConnackFixHead] = function(user, data, length, pos)
        local return_data = data:sub(length + pos)
        local session = data:byte(3)
        local reason_code = data:byte(4)
        log.info("Connack session", session, "reason", reason_code)
        if session == 0 and reason_code == 0 then
            user.keepalive_timer = sys.timerLoopStart(socket.tx, user.keepalive * 1000, user.netc, ping_req())
            user.user_cb(user, "connack")
        end
        return return_data
    end,
    [PublishFixHead] = function(user, data, length, pos)
        log.info("log.info test", length, pos)
        local return_data = data:sub(length + pos)
        local qos = data:byte(1, 1) & 0x06
        -- 主题
        local topicLen = string.sub(data, pos, pos + 1)
        topicLen = tonumber(topicLen:toHex(), 16)
        log.info("topic len", topicLen)
        pos = pos + 2
        local topic = string.sub(data, pos, pos + topicLen)
        pos = pos + topicLen

        -- 如果qos大于0, 则有2bytes标识符
        if qos > 0 then
            local identifier = string.sub(data, pos, pos + 1)
            pos = pos + 2
            if qos == 2 then
                socket.tx(user.netc, pack_puback(identifier))
            elseif qos == 4 then
                socket.tx(user.netc, pack_pubrec(identifier))
            end
        end
        -- 属性，如果有的话
        local property_len = string.byte(data, pos, pos)
        log.info("proerty len", property_len)
        pos = pos + 1 + property_len
        -- 负载
        local payload = string.sub(data, pos)
        user.user_cb(user, "recv", topic, payload)
        return return_data
    end,
    [SubackFixHead] = function(user, data, length, pos)
        local return_data = data:sub(length + pos)
        log.info("sub ", data:toHex(), length, pos)
        local len = string.sub(data, pos, pos + 1)
        return return_data
    end,

    [PingRespFixHead] = function(user, data, length, pos)
        local return_data = data:sub(length + pos)
        log.info("PingRespFixHead ", data:toHex(), length, pos)
        return return_data
    end,
    [PubrelFixHead] = function(user, data, length, pos)
        local return_data = data:sub(length + pos)
        log.info("PubrelFixHead ", data:toHex(), length, pos)
        return return_data
    end,
    [PubrecFixHead] = function(user, data, length, pos)
        local return_data = data:sub(length + pos)
        log.info("PubrecFixHead ", data:toHex(), length, pos)
        local id = data:sub(3, 4)
        socket.tx(user.netc, pack_pubrel(id))
        return return_data
    end,

    [DisconnectFixHead] = function(user, data, length, pos)
        local return_data = data:sub(length + pos)
        if user.keepalive_timer then
            sys.timerStop(user.keepalive_timer)
            user.keepalive_timer = nil
        end
        log.info("连接断开 ", data:toHex(), length, pos)
        return return_data
    end
}

local function mqtt_proc(opts)
    log.info("mqtt recv", opts.buf:sub(1, 50):toHex())
    local fix_head = opts.buf:byte(1)
    local length = 0
    local multiplier = 1
    local pos = 2
    repeat
        if pos > #opts.buf then
            return opts.buf
        end
        local digit = string.byte(opts.buf, pos)
        length = length + ((digit % 128) * multiplier)
        multiplier = multiplier * 128
        pos = pos + 1
    until digit < 128

    if #opts.buf < length + pos - 1 then
        log.info("data length not enough", #opts.buf, length + 2)
        return false, opts.buf
    end
    -- fix_head = (fix_head & 0xF0) >> 4
    if MqttPublicAnalysis[fix_head & 0xF0] then
        return true, MqttPublicAnalysis[fix_head & 0xF0](opts, opts.buf, length, pos)
    else
        log.info("id unregister", string.char(fix_head):toHex())
        return true, opts.buf:sub(length + pos)
    end
end

local function pack_connect(client_id, username, password, keepAlive, clean_session, will, property)
    local str = ""
    --- 固定报头
    -- str = str .. string.char(0x10)
    --- 可变报头
    -- 协议名
    -- MSB LSB M Q T T
    str = str .. string.char(0x00, 0x04) .. "MQTT"

    -- 协议版本 5
    str = str .. string.char(0x05)

    -- 连接标志
    -- bit7 username 
    -- bit6 password
    -- bit5 will_retain
    -- bit4、3 will_qos
    -- bit2 will flag
    -- bit1 clean start
    -- bit0 : reserved
    client_id = client_id and client_id or ""
    username = username and username or ""
    password = password and password or ""

    local connect_flag = (#username == 0 and 0 or 1) * 128 + (#password == 0 and 0 or 1) * 64 + (clean_session or 1) * 2
    if will and type(will) == "table" then
        connect_flag = connect_flag + will.retain * 32 + will.qos * 8 + 4
        log.info("Test", connect_flag)
    end

    str = str .. string.char(connect_flag)

    -- keepAlive
    str = str .. string.char(keepAlive // 256, keepAlive % 256)

    --- properties
    local properties = ""
    -- 主题别名最大长度
    if property and type(property) == "table" then
        if property.topic_alias_max_len then
            properties = properties .. string.char(0x22) .. string.char(property.topic_alias_max_len // 256, property.topic_alias_max_len % 256)
        end
    end

    str = str .. encode_len(#properties) .. properties

    --- payload
    if client_id and #client_id > 0 then
        str = str .. string.char(0x00, #client_id) .. client_id
    end

    -- will properties
    local will_data = ""
    if will and type(will) == "table" then
        properties = ""
        properties = string.char(0x01, 0x01)

        if will.property and type(will.property) == "table" then
            if will.property.delay_interval then
                local delay = string.char(0x18) .. string.pack(">L", will.property.delay_interval)
                properties = properties .. delay
            end
        end
        str = str .. encode_len(#properties) .. properties
        str = str .. string.char(0x00, #will.topic) .. will.topic
        str = str .. string.char(0x00, #will.payload) .. will.payload
    end

    -- username
    if username and #username > 0 then
        str = str .. string.char(0x00, #username) .. username
    end

    -- 长度
    str = string.char(ConnectFixHead) .. encode_len(#str) .. str
    log.info("tx data", str:toHex())
    return str
end

local function pack_subscribe(topic, qos)
    local str = ""
    --- 固定报头
    str = str .. string.char(SubscribeFixHead)

    -- 剩余长度
    -- str = str .. string.char(0x00, 0x00)

    -- 用户属性
    local property = ""
    property = string.char(0x00, 0x00, 0x00)
    -- local property = string.char(0x00, 0x0A, 0x00)

    topic = string.char(0x00, #topic) .. topic

    local option = string.char(qos)

    str = str .. encode_len(#property + #topic + #option) .. property .. topic .. qos
    return str
end

local function pack_publish(topic, payload, qos, retain, packet_id, property)
    local str = ""
    local topic_len = 0
    local identifier = ""
    local dup = 0
    if qos == 0 then
        qos = 0
    elseif qos == 1 then
        qos = 2
    elseif qos == 2 then
        qos = 4
    end
    if qos > 0 then
        identifier = string.char(packet_id // 256, packet_id % 256)
    end
    --- publish 报头
    str = str .. string.char(PublishFixHead + (dup + qos + retain))
    log.info("packet head", str:toHex())
    -- TOPIC NAME
    if topic and #topic > 0 then
        topic = string.char(0x00, #topic) .. topic
        topic_len = #topic
    else
        topic = string.char(0x00, 0x00)
        topic_len = 2
    end

    --- publish 属性
    local properties = ""
    -- 载荷格式指示 UTF8
    local protocol = string.char(0x01, 0x01)
    -- properties = string.char(#protocol) .. protocol
    properties = properties .. protocol
    -- 消息过期间隔 TODO

    -- 主题别名
    if property.alias then
        local alias = string.char(0x23) .. string.char(property.alias // 256, property.alias % 256)
        properties = properties .. alias
    end
    properties = encode_len(#properties) .. properties
    str = str .. encode_len(topic_len + #identifier + #properties + #payload) .. topic .. identifier .. properties .. payload
    log.info("tx data", str:toHex())
    return str
end

-- socket 回调函数
local function mqtt_socket_cb(opts, event)
    if event == socket.ON_LINE then
        -- TCP链接已建立, 那就可以上行了
        log.info("TCP connected")
        local str = pack_connect(opts.client_id, opts.username, opts.password, opts.keepalive, opts.clean_session, opts.will, opts.property)
        socket.tx(opts.netc, str)
    elseif event == socket.TX_OK then
        -- 数据传输完成
        log.info("TCP tx done")
    elseif event == socket.EVENT then
        local result = true
        while true do
            local succ, data_len = socket.rx(opts.netc, opts.rx_buff)
            log.info("TCP", succ, data_len)
            if succ and data_len > 0 then
                opts.buf = opts.buf .. opts.rx_buff:query()
                opts.rx_buff:del()
                log.info("recv data", data_len)
                while result and #opts.buf > 0 do
                    result, opts.buf = mqtt_proc(opts)
                end
            else
                break
            end
        end
    elseif event == socket.CLOSED then
        if opts.keepalive_timer then
            sys.timerStop(opts.keepalive_timer)
            opts.keepalive_timer = nil
        end
        log.info("tcp closed")
    end
end

function mqtt5.create(client_id, username, password, keepalive, clean_session, will, property)
    local opts = {}
    local netc = socket.create(nil, function(sc, event)
        if opts.netc then
            return mqtt_socket_cb(opts, event)
        end
    end)
    if not netc then
        log.error("创建socket失败了!!")
        return false
    end

    opts.netc = netc
    opts.rx_buff = zbuff.create(1024)
    opts.buf = ""
    opts.client_id = client_id
    opts.username = username or ""
    opts.password = password or ""
    opts.keepalive = keepalive or 240
    opts.clean_session = clean_session
    opts.will = will
    opts.property = property
    opts.packet_id = 0
    opts.next_id = function()
        opts.packet_id = opts.packet_id == 65535 and 1 or (opts.packet_id + 1)
        return opts.packet_id
    end
    setmetatable(opts, mqtt5)
    return opts
end

function mqtt5:on(cb)
    self.user_cb = cb
end

function mqtt5:connect(host, port)
    socket.config(self.netc, nil, nil)
    socket.connect(self.netc, host, port)
end

function mqtt5:subscribe(topic, qos)
    local str = pack_subscribe(topic, qos)
    socket.tx(self.netc, str)
end

function mqtt5:publish(topic, payload, qos, retain, property)
    local str = pack_publish(topic, payload, qos, retain, self.next_id(), property)
    socket.tx(self.netc, str)
end

function mqtt5:unsubscribe(topic)

end

return mqtt5
