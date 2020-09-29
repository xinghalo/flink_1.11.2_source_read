import { Overlay } from '@angular/cdk/overlay';
import { NzDrawerOptions } from './nz-drawer-options';
import { NzDrawerRef } from './nz-drawer-ref';
export declare class DrawerBuilderForService<R> {
    private overlay;
    private options;
    private drawerRef;
    private overlayRef;
    private unsubscribe$;
    constructor(overlay: Overlay, options: NzDrawerOptions);
    getInstance(): NzDrawerRef<R>;
    createDrawer(): void;
    updateOptions(options: NzDrawerOptions): void;
}
export declare class NzDrawerService {
    private overlay;
    constructor(overlay: Overlay);
    create<T = any, D = any, R = any>(options: NzDrawerOptions<T, D>): NzDrawerRef<R>;
}
