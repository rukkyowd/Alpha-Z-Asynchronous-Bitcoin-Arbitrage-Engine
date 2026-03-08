import EliteDashboard from "../components/EliteDashboard";

export default function Home() {
  return (
    <main className="relative min-h-screen w-full bg-[#06080d] text-slate-200">
      {/* Deep premium gradient overlays */}
      <div className="pointer-events-none fixed inset-0 z-0 bg-[radial-gradient(ellipse_at_top_right,_var(--tw-gradient-stops))] from-blue-900/15 via-[#06080d] to-[#06080d]" />
      <div className="pointer-events-none fixed inset-0 z-0 bg-[radial-gradient(ellipse_at_bottom_left,_var(--tw-gradient-stops))] from-emerald-900/10 via-transparent to-transparent" />
      
      <div className="relative z-10 w-full">
        <EliteDashboard />
      </div>
    </main>
  );
}