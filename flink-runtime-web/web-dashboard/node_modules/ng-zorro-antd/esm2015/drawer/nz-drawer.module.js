/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { PortalModule } from '@angular/cdk/portal';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzDrawerComponent } from './nz-drawer.component';
import { NzDrawerService } from './nz-drawer.service';
export class NzDrawerModule {
}
NzDrawerModule.decorators = [
    { type: NgModule, args: [{
                imports: [CommonModule, OverlayModule, PortalModule, NzIconModule, NzAddOnModule, NzNoAnimationModule],
                exports: [NzDrawerComponent],
                declarations: [NzDrawerComponent],
                entryComponents: [NzDrawerComponent],
                providers: [NzDrawerService]
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZHJhd2VyLm1vZHVsZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkcmF3ZXIvbnotZHJhd2VyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQztBQUNuRCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0saUJBQWlCLENBQUM7QUFDL0MsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUN6QyxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDM0QsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBRXRELE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDZDQUE2QyxDQUFDO0FBRWxGLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBQzFELE9BQU8sRUFBRSxlQUFlLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQztBQVN0RCxNQUFNLE9BQU8sY0FBYzs7O1lBUDFCLFFBQVEsU0FBQztnQkFDUixPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsYUFBYSxFQUFFLFlBQVksRUFBRSxZQUFZLEVBQUUsYUFBYSxFQUFFLG1CQUFtQixDQUFDO2dCQUN0RyxPQUFPLEVBQUUsQ0FBQyxpQkFBaUIsQ0FBQztnQkFDNUIsWUFBWSxFQUFFLENBQUMsaUJBQWlCLENBQUM7Z0JBQ2pDLGVBQWUsRUFBRSxDQUFDLGlCQUFpQixDQUFDO2dCQUNwQyxTQUFTLEVBQUUsQ0FBQyxlQUFlLENBQUM7YUFDN0IiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPdmVybGF5TW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHsgUG9ydGFsTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL3BvcnRhbCc7XG5pbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IE56QWRkT25Nb2R1bGUgfSBmcm9tICcuLi9jb3JlL2FkZG9uL2FkZG9uLm1vZHVsZSc7XG5pbXBvcnQgeyBOekljb25Nb2R1bGUgfSBmcm9tICcuLi9pY29uL256LWljb24ubW9kdWxlJztcblxuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5tb2R1bGUnO1xuXG5pbXBvcnQgeyBOekRyYXdlckNvbXBvbmVudCB9IGZyb20gJy4vbnotZHJhd2VyLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekRyYXdlclNlcnZpY2UgfSBmcm9tICcuL256LWRyYXdlci5zZXJ2aWNlJztcblxuQE5nTW9kdWxlKHtcbiAgaW1wb3J0czogW0NvbW1vbk1vZHVsZSwgT3ZlcmxheU1vZHVsZSwgUG9ydGFsTW9kdWxlLCBOekljb25Nb2R1bGUsIE56QWRkT25Nb2R1bGUsIE56Tm9BbmltYXRpb25Nb2R1bGVdLFxuICBleHBvcnRzOiBbTnpEcmF3ZXJDb21wb25lbnRdLFxuICBkZWNsYXJhdGlvbnM6IFtOekRyYXdlckNvbXBvbmVudF0sXG4gIGVudHJ5Q29tcG9uZW50czogW056RHJhd2VyQ29tcG9uZW50XSxcbiAgcHJvdmlkZXJzOiBbTnpEcmF3ZXJTZXJ2aWNlXVxufSlcbmV4cG9ydCBjbGFzcyBOekRyYXdlck1vZHVsZSB7fVxuIl19