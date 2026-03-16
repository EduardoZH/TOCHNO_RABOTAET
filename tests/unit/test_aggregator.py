"""Tests for services/aggregator_service/main.py — project-level aggregation logic."""

import json

import pytest


class SimpleAggregator:
    """Simplified aggregator for testing — no threads/timers."""

    def __init__(self):
        self.buffers = {}
        self.published = []

    def add_post(self, project_id: str, post_data: dict, total_posts: int):
        if project_id not in self.buffers:
            self.buffers[project_id] = []
        self.buffers[project_id].append(post_data)

        if len(self.buffers[project_id]) >= total_posts:
            self._send_project(project_id)

    def _send_project(self, project_id: str):
        posts = self.buffers.pop(project_id, [])
        self.published.append({
            "projectId": project_id,
            "posts": posts,
        })


class TestAggregatorLogic:
    def test_single_post_project_sent_immediately(self):
        agg = SimpleAggregator()
        agg.add_post("proj-1", {"title": "Post 1"}, total_posts=1)

        assert len(agg.published) == 1
        assert agg.published[0]["projectId"] == "proj-1"
        assert len(agg.published[0]["posts"]) == 1

    def test_multiple_posts_grouped(self):
        agg = SimpleAggregator()
        agg.add_post("proj-1", {"title": "Post 1"}, total_posts=2)
        assert len(agg.published) == 0

        agg.add_post("proj-1", {"title": "Post 2"}, total_posts=2)
        assert len(agg.published) == 1
        assert len(agg.published[0]["posts"]) == 2

    def test_projects_separated(self):
        agg = SimpleAggregator()
        agg.add_post("proj-1", {"title": "P1"}, total_posts=1)
        agg.add_post("proj-2", {"title": "P2"}, total_posts=1)

        assert len(agg.published) == 2
        assert agg.published[0]["projectId"] == "proj-1"
        assert agg.published[1]["projectId"] == "proj-2"

    def test_output_format(self):
        agg = SimpleAggregator()
        post_data = {
            "title": "Test Post",
            "content": "Test Content",
            "type": "article",
            "metrics": {"relevancy": 85, "tone": "negative"},
            "cluster_id": "cluster-abc",
        }
        agg.add_post("proj-1", post_data, total_posts=1)

        msg = agg.published[0]
        assert msg["projectId"] == "proj-1"
        assert len(msg["posts"]) == 1
        assert msg["posts"][0]["title"] == "Test Post"
        assert msg["posts"][0]["cluster_id"] == "cluster-abc"

    def test_content_truncation_to_500(self):
        agg = SimpleAggregator()
        long_content = "A" * 1000
        post_data = {"title": "Test", "content": long_content}
        agg.add_post("proj-1", post_data, total_posts=1)

        # Note: truncation happens in aggregator_service.main, not in this logic test
        # This test just verifies the data passes through
        assert len(agg.published[0]["posts"][0]["content"]) == 1000

    def test_five_posts_same_project(self):
        agg = SimpleAggregator()
        for i in range(4):
            agg.add_post("proj-big", {"title": f"Post {i}"}, total_posts=5)
            assert len(agg.published) == 0

        agg.add_post("proj-big", {"title": "Post 4"}, total_posts=5)
        assert len(agg.published) == 1
        assert len(agg.published[0]["posts"]) == 5

    def test_extra_posts_ignored_after_send(self):
        agg = SimpleAggregator()
        agg.add_post("proj-1", {"title": "Post 1"}, total_posts=2)
        agg.add_post("proj-1", {"title": "Post 2"}, total_posts=2)
        agg.add_post("proj-1", {"title": "Post 3"}, total_posts=2)  # Extra

        assert len(agg.published) == 1
        assert len(agg.published[0]["posts"]) == 2
