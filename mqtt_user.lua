local mqtt_user = {}
mqtt_user.__index = mqtt_user

local mqtt5 = require "mqtt5"

local function keep_alive(netc)
    socket.tx(netc, mqtt5.ping_req())
end

local function event_cb(event, user_param, param)
    local object = user_param
    local user_cb = object.user_cb
    local netc = object.netc
    if event == mqtt5.event_connack then
        local session = param.session
        local reason_code = param.reason_code
        if session == 0 and reason_code == 0 then
            user_cb(object, "connack")
            if not object.keepalive_timer then
                object.keepalive_timer = sys.timerLoopStart(keep_alive, object.keepalive * 1000, netc)
            end
        else
            user_cb(object, "disconnect")
        end
    elseif event == mqtt5.event_publish then
        if param.qos == 1 then
            socket.tx(netc, mqtt5.pack_puback(param.identifier))
        elseif param.qos == 2 then
            socket.tx(netc, mqtt5.pack_pubrec(param.identifier))
        end
        user_cb(object, "recv", param.topic, param.payload, nil, param.property)
    elseif event == mqtt5.event_pubrec then
        socket.tx(netc, mqtt5.pack_pubrel(param.identifier))
    elseif event == mqtt5.event_disconnect then
        if object.keepalive_timer then
            sys.timerStop(object.keepalive_timer)
            object.keepalive_timer = nil
        end
        user_cb(object, "disconnect")
    end
    log.info("event cb", event, user_param)
end

-- socket 回调函数
local function mqtt_socket_cb(opts, event)
    if event == socket.ON_LINE then
        -- TCP链接已建立, 那就可以上行了
        log.info("TCP connected")
        local str = mqtt5.pack_connect(opts.client_id, opts.username, opts.password, opts.keepalive, opts.clean_session, opts.will, opts.property)
        socket.tx(opts.netc, str)
    elseif event == socket.TX_OK then
        -- 数据传输完成
        -- log.info("TCP tx done")
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
                    result, opts.buf = mqtt5.mqtt_proc(opts.event_cb, opts.buf, opts)
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

function mqtt_user.create(client_id, username, password, keepalive, clean_session, will, property)
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
    opts.event_cb = event_cb
    opts.next_id = function()
        opts.packet_id = opts.packet_id == 65535 and 1 or (opts.packet_id + 1)
        return opts.packet_id
    end
    setmetatable(opts, mqtt_user)
    return opts
end

function mqtt_user:on(cb)
    self.user_cb = cb
end

function mqtt_user:connect(host, port)
    socket.config(self.netc, nil, nil)
    socket.connect(self.netc, host, port)
end

function mqtt_user:subscribe(topic, qos)
    local str = mqtt5.pack_subscribe(topic, qos)
    socket.tx(self.netc, str)
end

function mqtt_user:publish(topic, payload, qos, retain, property)
    local str = mqtt5.pack_publish(topic, payload, qos, retain, self.next_id(), property)
    socket.tx(self.netc, str)
end

function mqtt_user:unsubscribe(topic)

end

return mqtt_user
