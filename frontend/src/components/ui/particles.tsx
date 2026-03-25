/**
 * Particles — MagicUI (21st.dev community component)
 * Canvas-based particle system with mouse-following magnetic attraction.
 * https://magicui.design/r/particles
 */
import { useEffect, useRef, useState, useCallback } from "react";
import { cn } from "../../lib/utils";

interface ParticlesProps {
  className?: string;
  quantity?: number;
  staticity?: number;
  ease?: number;
  size?: number;
  color?: string;
  vx?: number;
  vy?: number;
  refresh?: boolean;
}

interface Circle {
  x: number;
  y: number;
  translateX: number;
  translateY: number;
  size: number;
  alpha: number;
  targetAlpha: number;
  dx: number;
  dy: number;
  magnetism: number;
}

function hexToRgb(hex: string): [number, number, number] {
  hex = hex.replace("#", "");
  if (hex.length === 3) {
    hex = hex
      .split("")
      .map((c) => c + c)
      .join("");
  }
  const num = parseInt(hex, 16);
  return [(num >> 16) & 255, (num >> 8) & 255, num & 255];
}

export function Particles({
  className = "",
  quantity = 100,
  staticity = 50,
  ease = 50,
  size = 0.4,
  color = "#ffffff",
  vx = 0,
  vy = 0,
  refresh = false,
}: ParticlesProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const canvasContainerRef = useRef<HTMLDivElement>(null);
  const context = useRef<CanvasRenderingContext2D | null>(null);
  const circles = useRef<Circle[]>([]);
  const mouse = useRef<{ x: number; y: number }>({ x: 0, y: 0 });
  const canvasSize = useRef<{ w: number; h: number }>({ w: 0, h: 0 });
  const dpr = typeof window !== "undefined" ? window.devicePixelRatio : 1;
  const [rgb, setRgb] = useState<[number, number, number]>(hexToRgb(color));

  useEffect(() => {
    setRgb(hexToRgb(color));
  }, [color]);

  const resizeCanvas = useCallback(() => {
    if (canvasContainerRef.current && canvasRef.current && context.current) {
      circles.current = [];
      canvasSize.current.w = canvasContainerRef.current.offsetWidth;
      canvasSize.current.h = canvasContainerRef.current.offsetHeight;
      canvasRef.current.width = canvasSize.current.w * dpr;
      canvasRef.current.height = canvasSize.current.h * dpr;
      canvasRef.current.style.width = `${canvasSize.current.w}px`;
      canvasRef.current.style.height = `${canvasSize.current.h}px`;
      context.current.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
  }, [dpr]);

  const circleParams = useCallback((): Circle => {
    const x = Math.floor(Math.random() * canvasSize.current.w);
    const y = Math.floor(Math.random() * canvasSize.current.h);
    const pSize = Math.floor(Math.random() * 2) + size;
    const alpha = 0;
    const targetAlpha = parseFloat((Math.random() * 0.6 + 0.1).toFixed(1));
    const dx = (Math.random() - 0.5) * 0.1;
    const dy = (Math.random() - 0.5) * 0.1;
    const magnetism = 0.1 + Math.random() * 4;
    return {
      x,
      y,
      translateX: 0,
      translateY: 0,
      size: pSize,
      alpha,
      targetAlpha,
      dx,
      dy,
      magnetism,
    };
  }, [size]);

  const drawCircle = useCallback(
    (circle: Circle, update = false) => {
      if (context.current) {
        const { x, y, translateX, translateY, size: s, alpha } = circle;
        context.current.translate(translateX, translateY);
        context.current.beginPath();
        context.current.arc(x, y, s, 0, 2 * Math.PI);
        context.current.fillStyle = `rgba(${rgb.join(", ")}, ${alpha})`;
        context.current.fill();
        context.current.setTransform(dpr, 0, 0, dpr, 0, 0);

        if (!update) {
          circles.current.push(circle);
        }
      }
    },
    [dpr, rgb]
  );

  const drawParticles = useCallback(() => {
    circles.current = [];
    for (let i = 0; i < quantity; i++) {
      const circle = circleParams();
      drawCircle(circle);
    }
  }, [circleParams, drawCircle, quantity]);

  const animate = useCallback(() => {
    if (!context.current) return;
    context.current.clearRect(
      0,
      0,
      canvasSize.current.w,
      canvasSize.current.h
    );

    circles.current.forEach((circle, i) => {
      // Handle edges
      const edgeDistances = {
        x: circle.x + circle.translateX - (mouse.current.x - canvasSize.current.w / 2),
        y: circle.y + circle.translateY - (mouse.current.y - canvasSize.current.h / 2),
      };
      const dist = Math.sqrt(edgeDistances.x ** 2 + edgeDistances.y ** 2);
      const clampedMagnetism = Math.max(0.01, circle.magnetism);

      if (dist < mouse.current.x && dist < mouse.current.y) {
        circle.alpha += 0.02;
        if (circle.alpha > circle.targetAlpha) circle.alpha = circle.targetAlpha;
      }

      circle.translateX +=
        (mouse.current.x / (staticity / clampedMagnetism) - circle.translateX) / ease + circle.dx + vx;
      circle.translateY +=
        (mouse.current.y / (staticity / clampedMagnetism) - circle.translateY) / ease + circle.dy + vy;

      // Fade in
      if (circle.alpha < circle.targetAlpha) {
        circle.alpha += 0.02;
      }

      // Edge wrapping
      if (
        circle.x + circle.translateX < 0 ||
        circle.x + circle.translateX > canvasSize.current.w ||
        circle.y + circle.translateY < 0 ||
        circle.y + circle.translateY > canvasSize.current.h
      ) {
        circle.alpha -= 0.02;
        if (circle.alpha <= 0) {
          circles.current[i] = circleParams();
          circles.current[i].alpha = 0.02;
        }
      }

      drawCircle(
        { ...circle, x: circle.x, y: circle.y },
        true
      );
    });
    requestAnimationFrame(animate);
  }, [circleParams, drawCircle, ease, staticity, vx, vy]);

  useEffect(() => {
    if (canvasRef.current) {
      context.current = canvasRef.current.getContext("2d");
    }
    resizeCanvas();
    drawParticles();
    const animationId = requestAnimationFrame(animate);

    const handleResize = () => {
      resizeCanvas();
      drawParticles();
    };
    window.addEventListener("resize", handleResize);

    return () => {
      cancelAnimationFrame(animationId);
      window.removeEventListener("resize", handleResize);
    };
  }, [animate, drawParticles, resizeCanvas, refresh]);

  const onMouseMove = useCallback((e: React.MouseEvent) => {
    if (canvasContainerRef.current) {
      const rect = canvasContainerRef.current.getBoundingClientRect();
      const { w, h } = canvasSize.current;
      const x = e.clientX - rect.left - w / 2;
      const y = e.clientY - rect.top - h / 2;
      mouse.current = { x, y };
    }
  }, []);

  return (
    <div
      className={cn("pointer-events-auto", className)}
      ref={canvasContainerRef}
      aria-hidden="true"
      onMouseMove={onMouseMove}
    >
      <canvas ref={canvasRef} className="size-full" />
    </div>
  );
}
