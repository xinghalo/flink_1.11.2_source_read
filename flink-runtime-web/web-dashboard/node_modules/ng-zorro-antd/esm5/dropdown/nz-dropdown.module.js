/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzButtonModule } from '../button/nz-button.module';
import { NzOverlayModule } from '../core/overlay/nz-overlay.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzMenuModule } from '../menu/nz-menu.module';
import { NzDropDownADirective } from './nz-dropdown-a.directive';
import { NzDropDownButtonComponent } from './nz-dropdown-button.component';
import { NzDropdownContextComponent } from './nz-dropdown-context.component';
import { NzDropDownComponent } from './nz-dropdown.component';
import { NzDropDownDirective } from './nz-dropdown.directive';
var NzDropDownModule = /** @class */ (function () {
    function NzDropDownModule() {
    }
    NzDropDownModule.decorators = [
        { type: NgModule, args: [{
                    imports: [
                        CommonModule,
                        OverlayModule,
                        FormsModule,
                        NzButtonModule,
                        NzMenuModule,
                        NzIconModule,
                        NzNoAnimationModule,
                        NzOverlayModule
                    ],
                    entryComponents: [NzDropdownContextComponent],
                    declarations: [
                        NzDropDownComponent,
                        NzDropDownButtonComponent,
                        NzDropDownDirective,
                        NzDropDownADirective,
                        NzDropdownContextComponent
                    ],
                    exports: [NzDropDownComponent, NzDropDownButtonComponent, NzDropDownDirective, NzDropDownADirective]
                },] }
    ];
    return NzDropDownModule;
}());
export { NzDropDownModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZHJvcGRvd24ubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImRyb3Bkb3duL256LWRyb3Bkb3duLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUU3QyxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSw2Q0FBNkMsQ0FBQztBQUVsRixPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDNUQsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLG1DQUFtQyxDQUFDO0FBQ3BFLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdEQsT0FBTyxFQUFFLG9CQUFvQixFQUFFLE1BQU0sMkJBQTJCLENBQUM7QUFDakUsT0FBTyxFQUFFLHlCQUF5QixFQUFFLE1BQU0sZ0NBQWdDLENBQUM7QUFDM0UsT0FBTyxFQUFFLDBCQUEwQixFQUFFLE1BQU0saUNBQWlDLENBQUM7QUFDN0UsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFDOUQsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFFOUQ7SUFBQTtJQXFCK0IsQ0FBQzs7Z0JBckIvQixRQUFRLFNBQUM7b0JBQ1IsT0FBTyxFQUFFO3dCQUNQLFlBQVk7d0JBQ1osYUFBYTt3QkFDYixXQUFXO3dCQUNYLGNBQWM7d0JBQ2QsWUFBWTt3QkFDWixZQUFZO3dCQUNaLG1CQUFtQjt3QkFDbkIsZUFBZTtxQkFDaEI7b0JBQ0QsZUFBZSxFQUFFLENBQUMsMEJBQTBCLENBQUM7b0JBQzdDLFlBQVksRUFBRTt3QkFDWixtQkFBbUI7d0JBQ25CLHlCQUF5Qjt3QkFDekIsbUJBQW1CO3dCQUNuQixvQkFBb0I7d0JBQ3BCLDBCQUEwQjtxQkFDM0I7b0JBQ0QsT0FBTyxFQUFFLENBQUMsbUJBQW1CLEVBQUUseUJBQXlCLEVBQUUsbUJBQW1CLEVBQUUsb0JBQW9CLENBQUM7aUJBQ3JHOztJQUM4Qix1QkFBQztDQUFBLEFBckJoQyxJQXFCZ0M7U0FBbkIsZ0JBQWdCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgT3ZlcmxheU1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7IENvbW1vbk1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQgeyBOZ01vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgRm9ybXNNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9mb3Jtcyc7XG5cbmltcG9ydCB7IE56Tm9BbmltYXRpb25Nb2R1bGUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24ubW9kdWxlJztcblxuaW1wb3J0IHsgTnpCdXR0b25Nb2R1bGUgfSBmcm9tICcuLi9idXR0b24vbnotYnV0dG9uLm1vZHVsZSc7XG5pbXBvcnQgeyBOek92ZXJsYXlNb2R1bGUgfSBmcm9tICcuLi9jb3JlL292ZXJsYXkvbnotb3ZlcmxheS5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJY29uTW9kdWxlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLm1vZHVsZSc7XG5pbXBvcnQgeyBOek1lbnVNb2R1bGUgfSBmcm9tICcuLi9tZW51L256LW1lbnUubW9kdWxlJztcbmltcG9ydCB7IE56RHJvcERvd25BRGlyZWN0aXZlIH0gZnJvbSAnLi9uei1kcm9wZG93bi1hLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOekRyb3BEb3duQnV0dG9uQ29tcG9uZW50IH0gZnJvbSAnLi9uei1kcm9wZG93bi1idXR0b24uY29tcG9uZW50JztcbmltcG9ydCB7IE56RHJvcGRvd25Db250ZXh0Q29tcG9uZW50IH0gZnJvbSAnLi9uei1kcm9wZG93bi1jb250ZXh0LmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekRyb3BEb3duQ29tcG9uZW50IH0gZnJvbSAnLi9uei1kcm9wZG93bi5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpEcm9wRG93bkRpcmVjdGl2ZSB9IGZyb20gJy4vbnotZHJvcGRvd24uZGlyZWN0aXZlJztcblxuQE5nTW9kdWxlKHtcbiAgaW1wb3J0czogW1xuICAgIENvbW1vbk1vZHVsZSxcbiAgICBPdmVybGF5TW9kdWxlLFxuICAgIEZvcm1zTW9kdWxlLFxuICAgIE56QnV0dG9uTW9kdWxlLFxuICAgIE56TWVudU1vZHVsZSxcbiAgICBOekljb25Nb2R1bGUsXG4gICAgTnpOb0FuaW1hdGlvbk1vZHVsZSxcbiAgICBOek92ZXJsYXlNb2R1bGVcbiAgXSxcbiAgZW50cnlDb21wb25lbnRzOiBbTnpEcm9wZG93bkNvbnRleHRDb21wb25lbnRdLFxuICBkZWNsYXJhdGlvbnM6IFtcbiAgICBOekRyb3BEb3duQ29tcG9uZW50LFxuICAgIE56RHJvcERvd25CdXR0b25Db21wb25lbnQsXG4gICAgTnpEcm9wRG93bkRpcmVjdGl2ZSxcbiAgICBOekRyb3BEb3duQURpcmVjdGl2ZSxcbiAgICBOekRyb3Bkb3duQ29udGV4dENvbXBvbmVudFxuICBdLFxuICBleHBvcnRzOiBbTnpEcm9wRG93bkNvbXBvbmVudCwgTnpEcm9wRG93bkJ1dHRvbkNvbXBvbmVudCwgTnpEcm9wRG93bkRpcmVjdGl2ZSwgTnpEcm9wRG93bkFEaXJlY3RpdmVdXG59KVxuZXhwb3J0IGNsYXNzIE56RHJvcERvd25Nb2R1bGUge31cbiJdfQ==