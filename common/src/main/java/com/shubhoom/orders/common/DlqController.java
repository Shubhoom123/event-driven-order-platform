package com.shubhoom.orders.common;

import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

// Unauthenticated admin endpoint: fine for a local demo, must sit behind auth in any real deployment.
@RestController
@RequestMapping("/admin/dlq")
class DlqController {

    private final DlqReplay replay;

    DlqController(DlqReplay replay) {
        this.replay = replay;
    }

    @PostMapping("/replay")
    Map<String, Object> replay(@RequestParam String topic) throws Exception {
        return Map.of("topic", topic, "replayed", replay.replay(topic));
    }
}
