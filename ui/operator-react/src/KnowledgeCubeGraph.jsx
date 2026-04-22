import React, { useEffect, useRef, useState } from "react";
import * as THREE from "three";
import { knowledgeTitleFromItem } from "./knowledgeVault.js";

const LATTICE_MIN_SIZE = 3;
const LATTICE_MAX_SIZE = 7;
const LATTICE_VISIBLE_POINT_COUNT = 200;
const EMPTY_SELECTED_KNOWLEDGE_IDS = [];

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function nodeId(node) {
  return node?.block_id || node?.knowledge_id || node?.id || "";
}

function knowledgeId(node) {
  return node?.knowledge_id || node?.id || node?.block_id || "";
}

function endpointCandidates(edge, side) {
  const prefix = side === "from" ? "from" : "to";
  return [
    edge?.[`${prefix}_block_id`],
    edge?.[`${prefix}_knowledge_id`],
    edge?.[`${prefix}_id`],
    edge?.[side],
    edge?.[side === "from" ? "source" : "target"],
  ].filter(Boolean);
}

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

function buildLatticePositions(pointCount) {
  const requestedPoints = Math.max(pointCount, LATTICE_MIN_SIZE ** 3);
  const dimension = clamp(Math.ceil(Math.cbrt(requestedPoints)), LATTICE_MIN_SIZE, LATTICE_MAX_SIZE);
  const spacing = 0.78;
  const offset = ((dimension - 1) * spacing) / 2;
  const positions = [];
  for (let z = 0; z < dimension; z += 1) {
    for (let y = 0; y < dimension; y += 1) {
      for (let x = 0; x < dimension; x += 1) {
        if (positions.length >= requestedPoints) {
          return positions;
        }
        positions.push(new THREE.Vector3(x * spacing - offset, y * spacing - offset, z * spacing - offset));
      }
    }
  }
  return positions;
}

function createLine(from, to, material) {
  const geometry = new THREE.BufferGeometry().setFromPoints([from, to]);
  return new THREE.Line(geometry, material);
}

function disposeObject(object) {
  const geometries = new Set();
  const materials = new Set();
  object.traverse((item) => {
    if (item.geometry) {
      geometries.add(item.geometry);
    }
    if (item.material) {
      const itemMaterials = Array.isArray(item.material) ? item.material : [item.material];
      itemMaterials.forEach((material) => materials.add(material));
    }
  });
  geometries.forEach((geometry) => geometry.dispose());
  materials.forEach((material) => material.dispose());
}

function clearGroup(group) {
  const children = [...group.children];
  children.forEach((child) => {
    group.remove(child);
    disposeObject(child);
  });
}

function resolveEndpointPosition(edge, side, positions) {
  for (const candidate of endpointCandidates(edge, side)) {
    const position = positions.get(candidate);
    if (position) {
      return position;
    }
  }
  return null;
}

function addLatticeGraph(contentGroup, nodes, edges, pickables) {
  const latticePositions = buildLatticePositions(Math.max(LATTICE_VISIBLE_POINT_COUNT, nodes.length));
  const inactiveMaterial = new THREE.MeshBasicMaterial({
    color: 0x27445b,
    transparent: true,
    opacity: 0.35,
  });
  const inactiveGeometry = new THREE.SphereGeometry(0.045, 12, 8);

  latticePositions.forEach((position) => {
    const mesh = new THREE.Mesh(inactiveGeometry, inactiveMaterial);
    mesh.position.copy(position);
    contentGroup.add(mesh);
  });

  const positions = new Map();
  const edgeMaterial = new THREE.LineBasicMaterial({
    color: 0x8ce7ff,
    transparent: true,
    opacity: 0.9,
  });
  const activeGeometry = new THREE.SphereGeometry(0.13, 24, 16);
  const activeMaterial = new THREE.MeshStandardMaterial({
    color: 0x7ae6ff,
    emissive: 0x39c8ff,
    emissiveIntensity: 1.35,
    roughness: 0.25,
    metalness: 0.18,
  });

  nodes.forEach((node, index) => {
    const id = nodeId(node);
    const idKnowledge = knowledgeId(node);
    const position = latticePositions[index] || stablePosition(index, nodes.length);
    positions.set(id, position);
    positions.set(idKnowledge, position);
    const mesh = new THREE.Mesh(activeGeometry, activeMaterial);
    mesh.position.copy(position);
    mesh.userData = { node, active: true };
    contentGroup.add(mesh);
    pickables.push(mesh);
  });

  edges.forEach((edge) => {
    const from = resolveEndpointPosition(edge, "from", positions);
    const to = resolveEndpointPosition(edge, "to", positions);
    if (from && to) {
      contentGroup.add(createLine(from, to, edgeMaterial));
    }
  });
}

function addSelectorGraph(contentGroup, nodes, edges, selectedKnowledgeIds, pickables) {
  const cube = new THREE.LineSegments(
    new THREE.EdgesGeometry(new THREE.BoxGeometry(5, 5, 5)),
    new THREE.LineBasicMaterial({ color: 0x5f7d92, transparent: true, opacity: 0.45 }),
  );
  contentGroup.add(cube);

  const selectedSet = new Set(Array.isArray(selectedKnowledgeIds) ? selectedKnowledgeIds : []);
  const positions = new Map();
  nodes.forEach((node, index) => {
    const id = nodeId(node);
    const idKnowledge = knowledgeId(node);
    const position = stablePosition(index, nodes.length);
    positions.set(id, position);
    positions.set(idKnowledge, position);
    const selected = selectedSet.has(idKnowledge);
    const mesh = new THREE.Mesh(
      new THREE.BoxGeometry(
        selected ? 0.26 : 0.18,
        selected ? 0.26 : 0.18,
        selected ? 0.26 : 0.18,
      ),
      new THREE.MeshStandardMaterial({
        color: selected ? 0x53d18f : 0x65a8ff,
        roughness: 0.4,
        metalness: 0.15,
      }),
    );
    mesh.position.copy(position);
    mesh.userData = { node, active: true };
    contentGroup.add(mesh);
    pickables.push(mesh);
  });

  edges.forEach((edge) => {
    const from = resolveEndpointPosition(edge, "from", positions);
    const to = resolveEndpointPosition(edge, "to", positions);
    if (from && to) {
      contentGroup.add(
        createLine(
          from,
          to,
          new THREE.LineBasicMaterial({ color: 0x9fb4c7, transparent: true, opacity: 0.52 }),
        ),
      );
    }
  });
}

function populateGraphContent(sceneState, graph, selectedKnowledgeIds, variant) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  const pickables = [];

  clearGroup(sceneState.contentGroup);
  if (variant === "lattice") {
    addLatticeGraph(sceneState.contentGroup, nodes, edges, pickables);
  } else {
    addSelectorGraph(sceneState.contentGroup, nodes, edges, selectedKnowledgeIds, pickables);
  }
  sceneState.pickables = pickables;
}

export function KnowledgeCubeGraph({
  graph,
  selectedKnowledgeIds = EMPTY_SELECTED_KNOWLEDGE_IDS,
  onSelect,
  variant = "selector",
}) {
  const mountRef = useRef(null);
  const sceneStateRef = useRef(null);
  const onSelectRef = useRef(onSelect);
  const [renderError, setRenderError] = useState("");
  const [hoveredNode, setHoveredNode] = useState(null);

  useEffect(() => {
    onSelectRef.current = onSelect;
  }, [onSelect]);

  useEffect(() => {
    const mount = mountRef.current;
    if (!mount) {
      return undefined;
    }

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x071018);
    const camera = new THREE.PerspectiveCamera(48, 1, 0.1, 100);
    camera.position.set(4.8, 4.2, 7.2);
    camera.lookAt(0, 0, 0);

    let renderer;
    try {
      renderer = new THREE.WebGLRenderer({
        antialias: true,
        preserveDrawingBuffer: true,
      });
      setRenderError("");
    } catch (error) {
      setRenderError(error?.message || "WebGL is unavailable.");
      return undefined;
    }

    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    mount.appendChild(renderer.domElement);

    const rootGroup = new THREE.Group();
    const contentGroup = new THREE.Group();
    const ambient = new THREE.AmbientLight(0xffffff, variant === "lattice" ? 1.35 : 1.6);
    const keyLight = new THREE.PointLight(0x9fd8ff, 2.2, 14);
    keyLight.position.set(3, 4, 6);
    rootGroup.add(ambient);
    rootGroup.add(keyLight);
    rootGroup.add(contentGroup);
    scene.add(rootGroup);

    const sceneState = {
      camera,
      contentGroup,
      pickables: [],
      renderer,
      rootGroup,
      scene,
    };
    sceneStateRef.current = sceneState;

    let dragging = false;
    let previousX = 0;
    let previousY = 0;
    let movedWhileDragging = false;
    const raycaster = new THREE.Raycaster();
    const pointer = new THREE.Vector2();

    const resize = () => {
      const width = Math.max(1, mount.clientWidth || 640);
      const height = Math.max(1, mount.clientHeight || 360);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
      renderer.setSize(width, height, false);
    };
    resize();
    const observer = window.ResizeObserver ? new ResizeObserver(resize) : null;
    observer?.observe(mount);

    const updateHover = (event) => {
      const rect = renderer.domElement.getBoundingClientRect();
      pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
      pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
      raycaster.setFromCamera(pointer, camera);
      const [hit] = raycaster.intersectObjects(sceneStateRef.current?.pickables || []);
      if (!hit?.object?.userData?.node) {
        setHoveredNode(null);
        return null;
      }
      const nextHoveredNode = {
        title: knowledgeTitleFromItem(hit.object.userData.node),
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
      };
      setHoveredNode(nextHoveredNode);
      return hit.object.userData.node;
    };

    const onPointerDown = (event) => {
      dragging = true;
      movedWhileDragging = false;
      previousX = event.clientX;
      previousY = event.clientY;
      renderer.domElement.setPointerCapture?.(event.pointerId);
    };
    const onPointerMove = (event) => {
      if (!dragging) {
        updateHover(event);
        return;
      }
      const dx = event.clientX - previousX;
      const dy = event.clientY - previousY;
      if (Math.abs(dx) + Math.abs(dy) > 1) {
        movedWhileDragging = true;
      }
      rootGroup.rotation.y += dx * 0.008;
      rootGroup.rotation.x += dy * 0.008;
      previousX = event.clientX;
      previousY = event.clientY;
      setHoveredNode(null);
    };
    const onPointerUp = (event) => {
      dragging = false;
      renderer.domElement.releasePointerCapture?.(event.pointerId);
    };
    const onPointerLeave = () => {
      dragging = false;
      setHoveredNode(null);
    };
    const onWheel = (event) => {
      event.preventDefault();
      const distance = clamp(camera.position.length() + event.deltaY * 0.008, 3, 13);
      camera.position.setLength(distance);
      camera.lookAt(0, 0, 0);
    };
    const onClick = (event) => {
      if (movedWhileDragging) {
        return;
      }
      const selectedNode = updateHover(event);
      if (selectedNode && onSelectRef.current) {
        onSelectRef.current(selectedNode);
      }
    };

    renderer.domElement.addEventListener("pointerdown", onPointerDown);
    renderer.domElement.addEventListener("pointermove", onPointerMove);
    renderer.domElement.addEventListener("pointerup", onPointerUp);
    renderer.domElement.addEventListener("pointerleave", onPointerLeave);
    renderer.domElement.addEventListener("wheel", onWheel, { passive: false });
    renderer.domElement.addEventListener("click", onClick);

    let frame = 0;
    const animate = () => {
      frame = requestAnimationFrame(animate);
      rootGroup.rotation.y += variant === "lattice" ? 0.0008 : 0.0012;
      renderer.render(scene, camera);
    };
    animate();

    return () => {
      cancelAnimationFrame(frame);
      observer?.disconnect();
      renderer.domElement.removeEventListener("pointerdown", onPointerDown);
      renderer.domElement.removeEventListener("pointermove", onPointerMove);
      renderer.domElement.removeEventListener("pointerup", onPointerUp);
      renderer.domElement.removeEventListener("pointerleave", onPointerLeave);
      renderer.domElement.removeEventListener("wheel", onWheel);
      renderer.domElement.removeEventListener("click", onClick);
      sceneStateRef.current = null;
      disposeObject(scene);
      renderer.dispose();
      if (mount.contains(renderer.domElement)) {
        mount.removeChild(renderer.domElement);
      }
    };
  }, [variant]);

  useEffect(() => {
    const sceneState = sceneStateRef.current;
    if (!sceneState) {
      return;
    }
    populateGraphContent(sceneState, graph, selectedKnowledgeIds, variant);
    setHoveredNode(null);
  }, [graph, selectedKnowledgeIds, variant]);

  return (
    <div className={`knowledge-cube-graph ${variant === "lattice" ? "is-lattice" : ""}`} ref={mountRef}>
      {hoveredNode ? (
        <div
          className="knowledge-cube-tooltip"
          style={{ left: `${hoveredNode.x}px`, top: `${hoveredNode.y}px` }}
        >
          {hoveredNode.title}
        </div>
      ) : null}
      {renderError ? <div className="knowledge-cube-fallback">{renderError}</div> : null}
    </div>
  );
}
