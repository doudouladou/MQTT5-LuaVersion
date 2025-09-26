
local mqtt5 = {}


local ConnectFixHead = 0x10
local ConnackFixHead = 0x20
local PublishFixHead = 0x30
local PubackFixHead = 0x40
local PubrecFixHead = 0x50
local PubrelFixHead = 0x62
local PubcompFixHead = 0x70
local SubscribeFixHead = 0x82
local SubackFixHead = 0x90
local UnsubscribeFixHead = 0xA2
local UnsubackFixHead = 0xB0
local PingReqFixHead = 0xC0
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

local function PingReq()
    local str = string.char(PingReqFixHead, 0x00)
    return str
end


local MqttPublicAnalysis = {
    [ConnackFixHead] = function(object, data, length, pos)
        local session = data:byte(3)
        local reason_code = data:byte(4)
        log.info("Connack session", session, "reason", reason_code)
        if session == 0 and reason_code == 0 then
            object.keepalive_timer = sys.timerLoopStart(socket.tx, object.keepalive * 1000, object.netc, PingReq())
            object.cb(object, "connack")
        end
        local data = data:sub(length + pos)
        return data
    end,
    [PublishFixHead] = function(object, data, length, pos)
        -- log.info("mqtt publish", len, #data)
        log.info("mqtt publish", length, pos)
        -- 主题
        local topicLen = string.sub(data, pos, pos + 1)
        -- log.info("主题长度", topicLen:toHex(), tonumber(topicLen:toHex(), 16))
        topicLen = tonumber(topicLen:toHex(), 16)
        pos = pos + 2
        local topic = string.sub(data, pos, pos + topicLen)
        -- log.info("主题", topic)
        pos = pos + topicLen

        -- 属性，如果有的话
        local pubPropertyLen = string.byte(data, pos, pos)
        pos = pos + 1 + pubPropertyLen

        -- 负载
        local payload = string.sub(data, pos)
        -- log.info("内容", pubPropertyLen, #payload, payload)
        object.cb(object, "recv", topic, payload)
        local data = data:sub(length + pos)
        return data
    end,
    [SubackFixHead] = function(object, data, length, pos)
        log.info("sub ", data:toHex(), length, pos)
        local data = data:sub(length + pos)
        return data
    end,

    [PingRespFixHead] = function(object, data, length, pos)
        log.info("心跳响应 ", data:toHex(), length, pos)
        local data = data:sub(length + pos)
        return data
    end,

    [DisconnectFixHead] = function(object, data, length, pos)
        if object.keepalive_timer then
            sys.timerStop(object.keepalive_timer)
            object.keepalive_timer = nil
        end
        log.info("连接断开 ", data:toHex(), length, pos)
        local data = data:sub(length + pos)
        return data
    end
}

local function mqtt_proc(opts)
    if #opts.buf < 50 then
        log.info("data", opts.buf:toHex())
    end
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

    if MqttPublicAnalysis[fix_head] then
        return true, MqttPublicAnalysis[fix_head](opts, opts.buf, length, pos)
    else
        log.info("id unregister", fix_head)
        return true, opts.buf:sub(length + pos)
    end
end

local function encode_utf8(s)
    if not s or #s == 0 then
        return ""
    else
        return string.pack(">P", s)
    end
end

local function pack_connect(client_id, username, password, keepAlive, cleanSession, will, property)
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
    username = username and username or ""

    local ConnectFlag = (cleanSession or 1) * 2 + ((username and #username > 0) and (128) or 0)

    str = str .. string.char(ConnectFlag)

    -- keepAlive
    str = str .. string.char(0x00, 0xff)

    --- properties
    local properties = ""
    -- 主题别名最大长度
    if property and type(property) == "table" then
        if property.topic_alias_max_len then
            properties = properties .. string.char(0x22).. string.char(property.topic_alias_max_len // 256, property.topic_alias_max_len % 256)
        end
    end
    

    str = str .. encode_len(#properties) .. properties

    if client_id and #client_id > 0 then
        str = str .. string.char(0x00, #client_id) .. client_id
    end

    -- username
    if username and #username > 0 then
        str = str .. string.char(0x00, #username) .. username
    end

    -- payload
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
    local property = string.char(0x00, 0x0A, 0x00)

    topic = string.char(0x00, #topic) .. topic

    local option = string.char(0x04)

    str = str .. encode_len(#property + #topic + #option) .. property .. topic .. option
    return str
end

local function pack_publish(topic, payload, qos, retain, property)
    local str = ""
    local topic_len = 0
    local dup = qos == 0 and 0 or 8
    if qos == 0 then
        qos = 0
    elseif qos == 1 then
        qos = 2
    elseif qos == 2 then
        qos = 4
    end
    --- publish 报头
    str = str .. string.char(PublishFixHead + (dup + qos + retain))
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
    str = str .. encode_len(topic_len + #properties + #payload) .. topic .. properties .. payload
    log.info("tx data", str:toHex())
    return str
end


-- socket 回调函数
local function mqtt_socket_cb(opts, event)
    if event == socket.ON_LINE then
        -- TCP链接已建立, 那就可以上行了
        log.info("TCP connected")
        local str = pack_connect(opts.client_id, opts.username, opts.password, opts.keepalive, opts.cleansession, opts.will, opts.property)
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



function mqtt5.create(client_id, username, password, keepalive, cleansession, will, property)
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
    opts.cleansession = cleansession
    opts.will = will
    opts.property = property
    return opts
end

function mqtt5.on(opts, cb)
    opts.cb = cb
end



function mqtt5.connect(opts, host, port)
    socket.config(opts.netc, nil, nil)
    socket.connect(opts.netc, host, port)
end

function mqtt5.subscribe(opts, topic, qos)
    local str = pack_subscribe(topic, qos)
    socket.tx(opts.netc, str)
end


function mqtt5.publish(opts, topic, payload, qos, retain, property)
    local str = pack_publish(topic, payload, qos, retain, property)
    socket.tx(opts.netc, str)
end

return mqtt5