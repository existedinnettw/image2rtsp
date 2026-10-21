#ifndef IMAGE2RTSP_IMAGE2RTSP_HPP
#define IMAGE2RTSP_IMAGE2RTSP_HPP

#include <gst/gst.h>
#include <gst/rtsp-server/rtsp-server.h>
#include <gst/app/gstappsrc.h>
#include <rclcpp/rclcpp.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "sensor_msgs/msg/image.hpp"
#include "sensor_msgs/msg/compressed_image.hpp"

// GStreamer video/x-raw format for a ROS image encoding
struct ImageFormat {
    const char *gst;
    bool reduce_to_8bit;   // 16-bit packed RGB/BGR: GStreamer has no such format, keep the high byte of each sample
};

// One RTSP mountpoint and its source: a ROS image topic, or a camera.
// Each stream keeps its own appsrc list and caps, so streams never share a lock:
// a stream's topic callback and its media's GStreamer callbacks meet only on appsrc_mutex.
struct Stream {
    explicit Stream(const rclcpp::Logger &logger) : logger(logger) {}
    ~Stream();

    rclcpp::Logger logger;
    std::string name;
    std::string topic;
    std::string mountpoint;
    std::string port;
    std::string pipeline;
    bool local_only = true;
    bool camera = false;
    bool compressed = false;
    unsigned int framerate = 30;

    // The appsrc of every prepared RTSP media: added in media_configure, removed in media_unprepared.
    std::vector<GstAppSrc*> appsrc_list;
    std::mutex appsrc_mutex;

    // Caps of the last pushed frame, rebuilt only when format or size change. Guarded by appsrc_mutex.
    GstCaps *caps = nullptr;
    std::string caps_format;
    int caps_width = 0;
    int caps_height = 0;

    rclcpp::CallbackGroup::SharedPtr callback_group;
    rclcpp::Subscription<sensor_msgs::msg::Image>::SharedPtr subscription_;
    rclcpp::Subscription<sensor_msgs::msg::CompressedImage>::SharedPtr subscription_compressed_;

    GstCaps *caps_for(const char *gst_format, int width, int height);
    void push_frame(GstBuffer *buf, GstCaps *frame_caps);
    void topic_callback(const sensor_msgs::msg::Image::SharedPtr msg);
    void compressed_topic_callback(const sensor_msgs::msg::CompressedImage::SharedPtr msg);
};

class Image2rtsp : public rclcpp::Node{
public:
    Image2rtsp();
    ~Image2rtsp() override;

    size_t stream_count() const { return streams.size(); }

private:
    // Without the `streams` parameter there is one stream, set by the top-level parameters
    // (topic, mountpoint, port, ...). With it, one stream per name, set by `<name>.topic` etc.,
    // each defaulting to its top-level parameter.
    std::vector<std::unique_ptr<Stream>> streams;

    // One RTSP server per port; the streams on a port are its mountpoints.
    struct Server {
        GstRTSPServer *server = nullptr;
        guint source_id = 0;
        bool local_only = true;
    };
    std::map<std::string, Server> servers;

    GMainLoop *main_loop = nullptr;
    std::thread main_loop_thread;
    guint cleanup_source_id = 0;

    std::unique_ptr<Stream> stream_from_parameters(const std::string &name);
    void subscribe(Stream &stream);
    void video_mainloop_start();
    GstRTSPServer *rtsp_server_for(const std::string &port, bool local_only);
    void rtsp_server_add_url(GstRTSPServer *server, Stream &stream);
    void log_stream_address(const Stream &stream);

    // GStreamer callbacks; user_data is the Stream (or a MediaCleanupData for media_prepared/media_unprepared,
    // or the Image2rtsp instance for session_cleanup)
    static void media_configure(GstRTSPMediaFactory *factory, GstRTSPMedia *media, gpointer user_data);
    static void media_prepared(GstRTSPMedia *media, gpointer user_data);
    static void media_unprepared(GstRTSPMedia *media, gpointer user_data);
    static gboolean session_cleanup(gpointer user_data);
};

unsigned int extract_framerate(const rclcpp::Logger &logger, const std::string &pipeline, unsigned int default_framerate);
std::string extract_device(const std::string &pipeline);
void probe_camera_device(const rclcpp::Logger &logger, const std::string &device);
const ImageFormat *format_from_encoding(const rclcpp::Logger &logger, const sensor_msgs::msg::Image &msg);

#endif // IMAGE2RTSP_IMAGE2RTSP_HPP
