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
var NzDrawerModule = /** @class */ (function () {
    function NzDrawerModule() {
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
    return NzDrawerModule;
}());
export { NzDrawerModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZHJhd2VyLm1vZHVsZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkcmF3ZXIvbnotZHJhd2VyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQztBQUNuRCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0saUJBQWlCLENBQUM7QUFDL0MsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUN6QyxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDM0QsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBRXRELE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDZDQUE2QyxDQUFDO0FBRWxGLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBQzFELE9BQU8sRUFBRSxlQUFlLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQztBQUV0RDtJQUFBO0lBTzZCLENBQUM7O2dCQVA3QixRQUFRLFNBQUM7b0JBQ1IsT0FBTyxFQUFFLENBQUMsWUFBWSxFQUFFLGFBQWEsRUFBRSxZQUFZLEVBQUUsWUFBWSxFQUFFLGFBQWEsRUFBRSxtQkFBbUIsQ0FBQztvQkFDdEcsT0FBTyxFQUFFLENBQUMsaUJBQWlCLENBQUM7b0JBQzVCLFlBQVksRUFBRSxDQUFDLGlCQUFpQixDQUFDO29CQUNqQyxlQUFlLEVBQUUsQ0FBQyxpQkFBaUIsQ0FBQztvQkFDcEMsU0FBUyxFQUFFLENBQUMsZUFBZSxDQUFDO2lCQUM3Qjs7SUFDNEIscUJBQUM7Q0FBQSxBQVA5QixJQU84QjtTQUFqQixjQUFjIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgT3ZlcmxheU1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7IFBvcnRhbE1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9wb3J0YWwnO1xuaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOekFkZE9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9hZGRvbi9hZGRvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJY29uTW9kdWxlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLm1vZHVsZSc7XG5cbmltcG9ydCB7IE56Tm9BbmltYXRpb25Nb2R1bGUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24ubW9kdWxlJztcblxuaW1wb3J0IHsgTnpEcmF3ZXJDb21wb25lbnQgfSBmcm9tICcuL256LWRyYXdlci5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpEcmF3ZXJTZXJ2aWNlIH0gZnJvbSAnLi9uei1kcmF3ZXIuc2VydmljZSc7XG5cbkBOZ01vZHVsZSh7XG4gIGltcG9ydHM6IFtDb21tb25Nb2R1bGUsIE92ZXJsYXlNb2R1bGUsIFBvcnRhbE1vZHVsZSwgTnpJY29uTW9kdWxlLCBOekFkZE9uTW9kdWxlLCBOek5vQW5pbWF0aW9uTW9kdWxlXSxcbiAgZXhwb3J0czogW056RHJhd2VyQ29tcG9uZW50XSxcbiAgZGVjbGFyYXRpb25zOiBbTnpEcmF3ZXJDb21wb25lbnRdLFxuICBlbnRyeUNvbXBvbmVudHM6IFtOekRyYXdlckNvbXBvbmVudF0sXG4gIHByb3ZpZGVyczogW056RHJhd2VyU2VydmljZV1cbn0pXG5leHBvcnQgY2xhc3MgTnpEcmF3ZXJNb2R1bGUge31cbiJdfQ==