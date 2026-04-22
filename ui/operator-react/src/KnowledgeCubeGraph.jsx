import React, { useEffect, useRef, useState } from "react";
import * as THREE from "three";

function stablePosition(index, total) {
  const phi = Math.acos(1 - (2 * (index + 0.5)) / Math.max(1, total));
  const theta = Math.PI * (1 + Math.sqrt(5)) * (index + 0.5);
  const radius = 2.1;
  return new THREE.Vector3(
    radius * Math.sin(phi) * Math.cos(theta),
    radius * Math.sin(phi) * Math.sin(theta),
    radius * Math.cos(phi),
  );
}

function nodeId(node) {
  return node?.block_id || node?.knowledge_id || node?.id || "";
}

export function KnowledgeCubeGraph({ graph, selectedKnowledgeIds = [], onSelect }) {
  const mountRef = useRef(null);
  const [renderError, setRenderError] = useState("");
  const selectedSet = new Set(Array.isArray(selectedKnowledgeIds) ? selectedKnowledgeIds : []);

  useEffect(() => {
    const mount = mountRef.current;
    if (!mount) {
      return undefined;
    }
    const width = mount.clientWidth || 640;
    const height = mount.clientHeight || 360;
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x071018);

    const camera = new THREE.PerspectiveCamera(48, width / height, 0.1, 100);
    camera.position.set(4, 4, 6);
    camera.lookAt(0, 0, 0);

    let renderer;
    try {
      renderer = new THREE.WebGLRenderer({ antialias: true });
      setRenderError("");
    } catch (error) {
      setRenderError(error?.message || "WebGL is unavailable.");
      return undefined;
    }
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.setSize(width, height);
    mount.appendChild(renderer.domElement);

    const group = new THREE.Group();
    scene.add(group);
    group.add(new THREE.AmbientLight(0xffffff, 1.6));

    const cube = new THREE.LineSegments(
      new THREE.EdgesGeometry(new THREE.BoxGeometry(5, 5, 5)),
      new THREE.LineBasicMaterial({ color: 0x5f7d92, transparent: true, opacity: 0.45 }),
    );
    group.add(cube);

    const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
    const edges = Array.isArray(graph?.edges) ? graph.edges : [];
    const positions = new Map();
    const pickables = [];
    nodes.forEach((node, index) => {
      const id = nodeId(node);
      const position = stablePosition(index, nodes.length);
      positions.set(id, position);
      const knowledgeId = node?.knowledge_id || id;
      const selected = selectedSet.has(knowledgeId);
      const mesh = new THREE.Mesh(
        new THREE.BoxGeometry(selected ? 0.26 : 0.18, selected ? 0.26 : 0.18, selected ? 0.26 : 0.18),
        new THREE.MeshStandardMaterial({
          color: selected ? 0x53d18f : 0x65a8ff,
          roughness: 0.4,
          metalness: 0.15,
        }),
      );
      mesh.position.copy(position);
      mesh.userData = { node };
      group.add(mesh);
      pickables.push(mesh);
    });

    edges.forEach((edge) => {
      const from = positions.get(edge?.from_block_id);
      const to = positions.get(edge?.to_block_id);
      if (!from || !to) {
        return;
      }
      const geometry = new THREE.BufferGeometry().setFromPoints([from, to]);
      group.add(
        new THREE.Line(
          geometry,
          new THREE.LineBasicMaterial({ color: 0x9fb4c7, transparent: true, opacity: 0.52 }),
        ),
      );
    });

    let dragging = false;
    let previousX = 0;
    let previousY = 0;
    const raycaster = new THREE.Raycaster();
    const pointer = new THREE.Vector2();

    const onPointerDown = (event) => {
      dragging = true;
      previousX = event.clientX;
      previousY = event.clientY;
    };
    const onPointerMove = (event) => {
      if (!dragging) {
        return;
      }
      const dx = event.clientX - previousX;
      const dy = event.clientY - previousY;
      group.rotation.y += dx * 0.008;
      group.rotation.x += dy * 0.008;
      previousX = event.clientX;
      previousY = event.clientY;
    };
    const onPointerUp = () => {
      dragging = false;
    };
    const onWheel = (event) => {
      event.preventDefault();
      camera.position.z = Math.max(3, Math.min(12, camera.position.z + event.deltaY * 0.006));
    };
    const onClick = (event) => {
      const rect = renderer.domElement.getBoundingClientRect();
      pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
      pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
      raycaster.setFromCamera(pointer, camera);
      const [hit] = raycaster.intersectObjects(pickables);
      if (hit?.object?.userData?.node && onSelect) {
        onSelect(hit.object.userData.node);
      }
    };

    renderer.domElement.addEventListener("pointerdown", onPointerDown);
    renderer.domElement.addEventListener("pointermove", onPointerMove);
    renderer.domElement.addEventListener("pointerup", onPointerUp);
    renderer.domElement.addEventListener("pointerleave", onPointerUp);
    renderer.domElement.addEventListener("wheel", onWheel, { passive: false });
    renderer.domElement.addEventListener("click", onClick);

    let frame = 0;
    const animate = () => {
      frame = requestAnimationFrame(animate);
      group.rotation.y += 0.0012;
      renderer.render(scene, camera);
    };
    animate();

    return () => {
      cancelAnimationFrame(frame);
      renderer.domElement.removeEventListener("pointerdown", onPointerDown);
      renderer.domElement.removeEventListener("pointermove", onPointerMove);
      renderer.domElement.removeEventListener("pointerup", onPointerUp);
      renderer.domElement.removeEventListener("pointerleave", onPointerUp);
      renderer.domElement.removeEventListener("wheel", onWheel);
      renderer.domElement.removeEventListener("click", onClick);
      renderer.dispose();
      mount.removeChild(renderer.domElement);
    };
  }, [graph, onSelect, selectedKnowledgeIds]);

  return (
    <div className="knowledge-cube-graph" ref={mountRef}>
      {renderError ? <div className="knowledge-cube-fallback">{renderError}</div> : null}
    </div>
  );
}
