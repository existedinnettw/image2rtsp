#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"
#include <gst/gst.h>
#include <gst/rtsp-server/rtsp-server.h>
#include <gst/app/gstappsrc.h>
#include "image2rtsp.hpp"
#include <stdexcept>
#include <string>
#ifdef __GLIBC__
#include <malloc.h>
#endif

using std::placeholders::_1;

Image2rtsp::Image2rtsp() : Node("image2rtsp"){
    // Declare and get the parameters
    this->declare_parameter("topic",        "/color/image_raw");
    this->declare_parameter("mountpoint",   "/back");
    this->declare_parameter("port",         "8554");
    this->declare_parameter("local_only",   true);
    this->declare_parameter("camera",       false);
    this->declare_parameter("compressed",   false);

    this->declare_parameter("default_pipeline",   R"(
                                                    ( appsrc name=imagesrc do-timestamp=true min-latency=0 max-latency=0 max-bytes=1000 is-live=true !
                                                    videoconvert !
                                                    videoscale !
                                                    video/x-raw, framerate=30/1, width=640, height=480 !
                                                    x264enc tune=zerolatency bitrate=500 key-int-max=30 !
                                                    video/x-h264, profile=baseline !
                                                    rtph264pay name=pay0 pt=96 )
                                                    )");

    this->declare_parameter("camera_pipeline",    R"(
                                                    ( v4l2src device=/dev/video0 !
                                                    videoconvert !
                                                    videoscale !
                                                    video/x-raw, framerate=30/1, width=640, height=480 !
                                                    x264enc tune=zerolatency bitrate=500 key-int-max=30 !
                                                    video/x-h264, profile=baseline !
                                                    rtph264pay name=pay0 pt=96 )
                                                    )");

    // Several streams in one process: a list of names, each configured by `<name>.topic`,
    // `<name>.mountpoint` etc., which default to the parameters above. Without it, the
    // parameters above are the one stream.
    auto names = this->declare_parameter("streams", std::vector<std::string>{});
    if (names.empty()){
        streams.push_back(stream_from_parameters(""));
    }
    for (const auto &name : names){
        if (name.empty() || name.find_first_of(". ") != std::string::npos){
            throw std::runtime_error("Invalid stream name '" + name + "' in streams");
        }
        for (const auto &other : streams){
            if (other->name == name) throw std::runtime_error("Stream '" + name + "' is listed twice in streams");
        }
        streams.push_back(stream_from_parameters(name));
    }

    // The streams on one port share one RTSP server, so they need distinct mountpoints and the same local_only
    for (size_t i = 0; i < streams.size(); i++){
        for (size_t j = 0; j < i; j++){
            const Stream &a = *streams[j], &b = *streams[i];
            if (a.port != b.port) continue;
            if (a.mountpoint == b.mountpoint){
                throw std::runtime_error("Streams '" + a.name + "' and '" + b.name + "' both serve " + a.port + a.mountpoint);
            }
            if (a.local_only != b.local_only){
                throw std::runtime_error("Streams '" + a.name + "' and '" + b.name + "' share port " + a.port + " but not local_only");
            }
        }
    }

    // Start the subscriptions
    for (auto &stream : streams) subscribe(*stream);

    // Set up the RTSP servers. The GStreamer main loop thread is started last, so a
    // failure here (e.g. a port is in use) can still be reported by throwing.
    gst_init(NULL, NULL);

    // With a camera source nothing touches the device until the first client sends
    // DESCRIBE, so a busy or missing device used to surface only as a 503 on the
    // client with no trace in the ROS log. Check it up front and refuse to start.
    for (auto &stream : streams){
        if (!stream->camera) continue;
        std::string device = extract_device(stream->pipeline);
        if (device.empty()){
            RCLCPP_WARN(stream->logger, "camera is true but camera_pipeline has no 'device=': skipping the device check");
        } else {
            RCLCPP_INFO(stream->logger, "Checking camera device %s", device.c_str());
            probe_camera_device(stream->logger, device);
            RCLCPP_INFO(stream->logger, "Camera device %s is available", device.c_str());
        }
    }

    for (auto &stream : streams){
        rtsp_server_add_url(rtsp_server_for(stream->port, stream->local_only), *stream);
    }
    /* periodically remove expired sessions */
    cleanup_source_id = g_timeout_add_seconds(2, session_cleanup, this);
    video_mainloop_start();

    for (auto &stream : streams) log_stream_address(*stream);
}

/* The stream `name` from its `<name>.` parameters, or with an empty name the stream of the top-level ones. */
std::unique_ptr<Stream> Image2rtsp::stream_from_parameters(const std::string &name){
    auto stream = std::make_unique<Stream>(name.empty() ? this->get_logger() : this->get_logger().get_child(name));
    stream->name = name;

    // value: the top-level parameter, which is the stream's own default
    auto get = [&](const std::string &key, auto value){
        using T = decltype(value);
        return name.empty() ? value : this->declare_parameter<T>(name + "." + key, value);
    };
    stream->topic       = get("topic",      this->get_parameter("topic").as_string());
    stream->mountpoint  = get("mountpoint", this->get_parameter("mountpoint").as_string());
    stream->port        = get("port",       this->get_parameter("port").as_string());
    stream->local_only  = get("local_only", this->get_parameter("local_only").as_bool());
    stream->camera      = get("camera",     this->get_parameter("camera").as_bool());
    stream->compressed  = get("compressed", this->get_parameter("compressed").as_bool());
    std::string default_pipeline = get("default_pipeline", this->get_parameter("default_pipeline").as_string());
    std::string camera_pipeline  = get("camera_pipeline",  this->get_parameter("camera_pipeline").as_string());

    stream->pipeline = stream->camera ? camera_pipeline : default_pipeline;
    stream->framerate = extract_framerate(stream->logger, stream->pipeline, 30);
    return stream;
}

void Image2rtsp::subscribe(Stream &stream){
    if (stream.camera) return;
    /* Each stream's callbacks run one at a time, but in parallel with the other streams' under a multi-threaded executor */
    stream.callback_group = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
    rclcpp::SubscriptionOptions options;
    options.callback_group = stream.callback_group;
    if (stream.compressed == false){
        stream.subscription_ = this->create_subscription<sensor_msgs::msg::Image>(stream.topic, 10, std::bind(&Stream::topic_callback, &stream, _1), options);
        RCLCPP_INFO(stream.logger, "Subscribing to sensor_msgs::msg::Image");
    }
    else {
        stream.subscription_compressed_ = this->create_subscription<sensor_msgs::msg::CompressedImage>(stream.topic, 10, std::bind(&Stream::compressed_topic_callback, &stream, _1), options);
        RCLCPP_INFO(stream.logger, "Subscribing to sensor_msgs::msg::CompressedImage");
    }
}

void Image2rtsp::log_stream_address(const Stream &stream){
    gchar *server_address = gst_rtsp_server_get_address(servers.at(stream.port).server);
    if (stream.local_only) {
        RCLCPP_INFO(stream.logger, "Stream available at rtsp://%s:%s%s", server_address, stream.port.c_str(), stream.mountpoint.c_str());
    } else {
        RCLCPP_INFO(stream.logger, "RTSP server bound to %s:%s%s", server_address, stream.port.c_str(), stream.mountpoint.c_str());
        RCLCPP_INFO(stream.logger, "Connect clients using rtsp://<host-ip>:%s%s (0.0.0.0 is bind-only)", stream.port.c_str(), stream.mountpoint.c_str());
    }
    g_free(server_address);
}

unsigned int extract_framerate(const rclcpp::Logger &logger, const std::string& pipeline, unsigned int default_framerate) {
    std::string search_str = "framerate=";
    size_t pos = pipeline.find(search_str);
    if (pos == std::string::npos) {
        RCLCPP_WARN(logger, "Framerate not found in pipeline, using default: %u", default_framerate);
        return default_framerate;
    }

    pos += search_str.length();

    size_t end_pos = pipeline.find_first_of("/,", pos);
    if (end_pos == std::string::npos) {
        RCLCPP_WARN(logger, "Invalid framerate format in pipeline, using default: %u", default_framerate);
        return default_framerate;
    }

    std::string framerate_str = pipeline.substr(pos, end_pos - pos);

    framerate_str.erase(0, framerate_str.find_first_not_of(" \t"));
    framerate_str.erase(framerate_str.find_last_not_of(" \t") + 1);
    
    try {
        int framerate = std::stoi(framerate_str);
        if (framerate <= 0) {
            RCLCPP_WARN(logger, "Invalid framerate value %d, using default: %u", framerate, default_framerate);
            return default_framerate;
        }
        RCLCPP_INFO(logger, "Using set framerate %d", framerate);
        return static_cast<unsigned int>(framerate);
    } catch (const std::exception& e) {
        RCLCPP_WARN(logger, "Failed to parse framerate '%s', using default: %u", framerate_str.c_str(), default_framerate);
        return default_framerate;
    }
}

/* Pull the "device=..." value out of a gst-launch pipeline description. */
std::string extract_device(const std::string &pipeline){
    const std::string key = "device=";
    size_t pos = pipeline.find(key);
    if (pos == std::string::npos) return "";
    pos += key.length();
    size_t end = pipeline.find_first_of(" \t\r\n!", pos);
    if (end == std::string::npos) end = pipeline.length();
    std::string device = pipeline.substr(pos, end - pos);
    /* tolerate device="/dev/video0" */
    if (device.size() >= 2 && device.front() == '"' && device.back() == '"') device = device.substr(1, device.size() - 2);
    return device;
}

/* Grab a single frame from the device so that an unusable camera (busy, missing, or
 * without a usable format) stops the node instead of failing silently per client.
 * Throws std::runtime_error, which main() turns into a FATAL log and exit code 1. */
void probe_camera_device(const rclcpp::Logger &logger, const std::string &device){
    std::string desc = "v4l2src device=" + device + " num-buffers=1 ! fakesink sync=false";
    GError *parse_error = nullptr;
    GstElement *probe = gst_parse_launch(desc.c_str(), &parse_error);
    if (parse_error){
        std::string msg = "Could not build the camera probe pipeline: " + std::string(parse_error->message);
        g_error_free(parse_error);
        if (probe) gst_object_unref(probe);
        throw std::runtime_error(msg);
    }

    gst_element_set_state(probe, GST_STATE_PLAYING);
    GstBus *bus = gst_element_get_bus(probe);
    /* EOS: the frame arrived and the device works. ERROR: it does not. */
    GstMessage *msg = gst_bus_timed_pop_filtered(bus, 5 * GST_SECOND,
                                                 (GstMessageType)(GST_MESSAGE_ERROR | GST_MESSAGE_EOS));
    std::string failure;
    bool timed_out = (msg == nullptr);
    if (msg && GST_MESSAGE_TYPE(msg) == GST_MESSAGE_ERROR){
        GError *err = nullptr;
        gchar *debug = nullptr;
        gst_message_parse_error(msg, &err, &debug);
        failure = "Camera device " + device + " is not usable: " + (err ? err->message : "unknown error");
        if (debug) failure += std::string(" (") + debug + ")";
        if (err) g_error_free(err);
        g_free(debug);
    }
    if (msg) gst_message_unref(msg);
    gst_element_set_state(probe, GST_STATE_NULL);
    gst_object_unref(bus);
    gst_object_unref(probe);

    /* A timeout is ambiguous (a slow camera looks the same as a stuck one), so warn
     * rather than refuse to start; a reported error is definitive and is fatal. */
    if (timed_out){
        RCLCPP_WARN(logger, "No frame from %s within 5 s; starting anyway, the stream may not work", device.c_str());
    }
    if (!failure.empty()) throw std::runtime_error(failure);
}

int main(int argc, char *argv[]){
#ifdef __GLIBC__
    // Every RTSP media pipeline starts a new set of threads, and glibc gives each
    // thread its own malloc arena whose freed memory is not returned to the OS.
    // With clients connecting and disconnecting this grew by ~5 MB per cycle;
    // a single arena keeps it flat at no measurable CPU cost. Must run before
    // any thread is created.
    mallopt(M_ARENA_MAX, 1);
#endif
    rclcpp::init(argc, argv);
    int rc = 0;
    try {
        auto node = std::make_shared<Image2rtsp>();
        if (node->stream_count() > 1){
            // A thread per stream, as with a process per stream: each stream's callbacks
            // stay in order in its own callback group, and no stream waits for another's.
            rclcpp::executors::MultiThreadedExecutor executor(rclcpp::ExecutorOptions(), node->stream_count());
            executor.add_node(node);
            executor.spin();
        } else {
            rclcpp::spin(node);
        }
    } catch (const std::exception &e) {
        RCLCPP_FATAL(rclcpp::get_logger("image2rtsp"), "%s", e.what());
        rc = 1;
    }
    rclcpp::shutdown();
    return rc;
}
